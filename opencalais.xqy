xquery version "1.0-ml";

module namespace oc = "http://marklogic.com/opencalais";

declare namespace http = "xdmp:http";
declare namespace rdf  ="http://www.w3.org/1999/02/22-rdf-syntax-ns#";

declare default function namespace "http://www.w3.org/2005/xpath-functions";

declare option xdmp:mapping "false";

declare variable $memcache as map:map := map:map();

declare variable $cache-root as xs:string := "/opencalais-cache/";
declare variable $timeout as xs:int := 600; (: seconds :)
declare variable $max-tries as xs:int := 3;
declare variable $retry-delay as xs:int := 2000; (: millisec :)

declare function oc:setCacheRoot($new-root as xs:string) as empty-sequence() {
  xdmp:set($cache-root, $new-root)
};

declare function oc:enrich($uri as xs:string, $data as node(), $license as xs:string+) as element(rdf:RDF)? {
  oc:enrich($uri, $data, $license, ())
};

declare function oc:enrich($uri as xs:string, $data as node(), $license as xs:string+, $language as xs:string?) as element(rdf:RDF)? {
  let $rdf := oc:getFromCache($uri)
  return
  if ($rdf) then (
    xdmp:log(concat("Pulled ", $uri, " from cache")),
    $rdf
  ) else
    let $rdf := oc:get($uri, $data, $license, $language, 1)
    return
    if ($rdf) then (
      xdmp:log(concat("Retrieved ", $uri, " from OpenCalais")),
      oc:putInCache($uri, $rdf),
      $rdf
    ) else (
      xdmp:log(concat("Failed to retrieve ", $uri, " from OpenCalais"))
    )
};

declare function oc:persistCache() {
  oc:persistCache(xdmp:default-permissions(), xdmp:default-collections())
};

declare function oc:persistCache($document-permissions, $collections) {
  for $uri in map:keys($memcache)
  return (
    xdmp:log(concat("Persisting ", $uri, " to database..")),
    xdmp:document-insert($uri, map:get($memcache, $uri), $document-permissions, $collections)
  )
};

declare private function oc:get($uri as xs:string, $data as node(), $license as xs:string+, $language as xs:string?, $tries as xs:int) as element(rdf:RDF)? {
  let $response :=
    try {
      xdmp:http-post("https://api.thomsonreuters.com/permid/calais",
      <options xmlns="xdmp:http">
        <timeout>{$timeout}</timeout>
        <headers>
          <x-ag-access-token>{$license[1]}</x-ag-access-token>
          {
            if ($language) then
              <x-calais-language>{$language}</x-calais-language>
            else ()
          }
          <content-type>text/raw</content-type>
          <outputFormat>xml/rdf</outputFormat>
        </headers>
        <data>{xdmp:quote($data)}</data>
        <format xmlns="xdmp:document-get">text</format>
      </options>
      )
    } catch ($e) {
      $e
    }
  return
    (: check for errors :)
    if ($response[1] instance of element(error:error) or $response[1]/http:code ge 400) then (
      if ($response[1]/http:code eq 429 or contains($response[2], "403 Developer Over Qps")) then (
        if (count($license) gt 1) then (
          xdmp:log(concat("Rate limit exceeded for ", $uri, ", trying again with next license..")),
          xdmp:sleep(500),
          oc:get($uri, $data, tail($license), "English", $tries)
        ) else (
          if (not(contains($response[2], "requests per day")) and ($tries lt $max-tries)) then (
            xdmp:log(concat("Rate limit exceeded for ", $uri, ", trying again in ", $retry-delay * $tries div 1000, " sec..")),
            xdmp:sleep($retry-delay * $tries),
            oc:get($uri, $data, $license, $language, $tries + 1)
          ) else (
            xdmp:log(concat("Giving up on ", $uri, " after ", $tries, " retries.."))
          )
        )
      ) else if (not($language = "English") and contains($response[2], "Calais continues to expand its list of supported languages")) then (
        xdmp:log(concat("Unrecognized language ", $language, " for ", $uri, ", trying again with English..")),
        xdmp:sleep(500),
        oc:get($uri, $data, $license, "English", $tries + 1)
      ) else (
        xdmp:log(($uri, xdmp:describe($data), $license, $language, $response))
      )
    ) else (
      xdmp:unquote($response[2])/rdf:RDF
    )
};

declare private function oc:getFromCache($uri as xs:string) as element(rdf:RDF)? {
  let $uri := concat($cache-root, encode-for-uri(encode-for-uri($uri)), ".xml")
  let $inmem := map:get($memcache, $uri)
  return
  if ($inmem) then
    $inmem
  else
    doc($uri)/rdf:RDF
};

declare private function oc:putInCache($uri as xs:string, $rdf as element(rdf:RDF)) as empty-sequence() {
  let $uri := concat($cache-root, encode-for-uri(encode-for-uri($uri)), ".xml")
  return
    map:put($memcache, $uri, $rdf)
};

