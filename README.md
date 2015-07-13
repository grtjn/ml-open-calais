# ml-open-calais
MarkLogic XQuery library for using Thomson Reuters Open Calais™ API

Note: Open Calais allows making about 5000 calls per day for free, a few per second. If using this code from inside an MLCP transform, make sure to pace it down to single thread with --nr_threads 1 --transaction_size 1 --batch_size 1

## Install

Installation depends on the [MarkLogic Package Manager](https://github.com/joemfb/mlpm):

```
$ mlpm install ml-open-calais --save
$ mlpm deploy
```

## Usage

```xquery
xquery version "1.0-ml";

import module namespace oc = "http://marklogic.com/opencalais";

let $oc-license := "..."
let $article-uri := "http://developer.marklogic.com/blog/FirstJSONDoc"
let $article :=
  xdmp:tidy(xdmp:http-get($article-uri)[2])[2]
    //*:div[string(@id) = "main"]
return (
  $article,
  oc:enrich($article-uri, $article, $oc-license, "English")
),

oc:persistCache()
```
