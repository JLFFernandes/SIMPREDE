The Arquivo.pt API allows full-text search and access preserved web content and related metadata. It is also possible to search by URL, accessing all versions of preserved web content.  
API returns a **JSON** object.

**EndPoint**: [https://arquivo.pt/textsearch](https://arquivo.pt/textsearch)

**Note:** a text search query returns a **maximum of 500 response results**

# Request Parameters

### Search for terms

| **Parameter Name** | **Description** | **Examples** |
|:------|:-------------|:----------|
| q | Query search terms. <br>The query can contain advanced search operators. Advanced search operators can be:  <ul><li> " " : search for items containing expression composed by search terms (e.g. phrase or named entity).</li><li> - : excludes items that contains the given terms. </li></ul> Do not input URLs in parameter **q** because it will return an HTTP 400 Bad request error. Use **versionHistory** parameter to search for URLs or alternative APIs that support URL search: [CDX-server API](https://github.com/arquivo/pwa-technologies/wiki/URL-search:-CDX-server-API), [Memento API](https://github.com/arquivo/pwa-technologies/wiki/Memento--API).| [q=James Davis](https://arquivo.pt/textsearch?q=James%20Davis&prettyPrint=true) &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;<br>[q="Antonio Costa"](https://arquivo.pt/textsearch?q="António%20Costa"&prettyPrint=true)<br>[q=Albert -Einstein](https://arquivo.pt/textsearch?q=Albert%20-Einstein&prettyPrint=true)|
| from | Set an initial date for the time span of the search. <br>Format: YYYYMMDDHHMMSS, also accepts a shorter date fotmat, e.g. (YYYY). <br>Default: 1996 | [from=19960101000000](https://arquivo.pt/textsearch?q=soccer&from=19960101000000) | 
| to | Set a end date for the time span of the search.<br>Format: YYYYMMDDHHMMSS, also accepts a shorter date format, for example (YYYY).<br>Default: Current Year-1 | [to=20151022163016](https://arquivo.pt/textsearch?q=soccer&to=20151022163016) |
| type | Specify accepted formats for the response items. <br>Subtype of the MIME types (e.g. pdf, ps, html, xls, ppt, doc, rtf, etc). | [type=pdf](https://arquivo.pt/textsearch?q=euro&type=pdf&prettyPrint=true) | 
| offset | The position of the text indices where the search begins.<br>Default:**0** | [offset=0](https://arquivo.pt/textsearch?q=soccer&offset=0) |
| siteSearch | Limit search within a given site. | [siteSearch=http&#58;//www.publico.pt](https://arquivo.pt/textsearch?q=Obama&siteSearch=http://www.publico.pt&prettyPrint=true) |
| collection | Limit search within a given collection. Only results from the specified collections are return. [A list of all the collections preserved by Arquivo.pt is publicly available](https://docs.google.com/spreadsheets/u/2/d/e/2PACX-1vSwVV3LqlmS7Ia4cFEO85cWr8Ip16TxMXCWFGPxVBCJhlpfkdqT45ykjDx3zLiYXsL3mC6OZuVyqwYS/pubhtml?gid=0&single=true) | [collection=EAWP13,EAWP21](https://arquivo.pt/textsearch?q=war&collection=EAWP21,EAWP13) |
| maxItems |  Maximum number of items on the response. <br>Default: **50**.<br> Max: **500** | [maxItems=50](https://arquivo.pt/textsearch?q=soccer&maxItems=50) |
| itemsPerSite (**deprecated**) | Maximum number of items per each site. (site)<br>Default: itemsPerSite = 2<br> This parameter is **deprecated** by the **dedupValue** parameter | [itemsPerSite=5](https://arquivo.pt/textsearch?q=soccer&itemsPerSite=5&prettyPrint=true) |
| dedupValue | Maximum number of items per dedupField. (The default **dedupField** used is "site") | [dedupValue=5](https://arquivo.pt/textsearch?q=soccer&dedupValue=5&prettyPrint=true) |
| dedupField | Result field where the deduplication will be performed. (ex. site, url) | [dedupField=site](https://arquivo.pt/textsearch?q=soccer&dedupField=url&prettyPrint=true) |
| fields | Selector specifying a subset of fields to include in the response. Separated by ",". <br>Possible fields: title, originalURL, linkToArchive, tstamp, contentLength, digest, mimeType, linkToScreenshot, date, encoding, linkToNoFrame, linkToOriginalFile, collection, snippet, linkToExtractedText | [fields=title, originalURL, linkToArchive, tstamp](https://arquivo.pt/textsearch?q=soccer&fields=title,originalURL,linkToArchive,tstamp&prettyPrint=true) |
| callback | Callback function. <ul><li>For more information, see the partial request section in the [REST from JavaScript](#rest-from-javascript). </li><li>Use for better performance.</li></ul> | [callback=hndlr](https://arquivo.pt/textsearch?q=soccer&callback=hndlr&prettyPrint=true) |
| prettyPrint | Returns response with indentations and line breaks. <ul><li>Returns the response in a human-readable format if true. </li><li>Default value: true.</li><li>When this is false, it can reduce the response payload size, which might lead to better performance in some environments. </li></ul> | [prettyPrint=true](https://arquivo.pt/textsearch?q=soccer&prettyPrint=true) |


### Search for URL

Search by URL, allows automatic access to all preserved versions of a respective preserved URL. The items are returned from the most recent to the old one.

| **Parameter name** | **Description** | **Examples** |
|:------|:--------------------------------------------|:--------|
| versionHistory |  The only parameter required for URL search.<br> It will return a list of the preserved versions for the URL that was required.<br> The URL may or may not contain the protocol, eg. http. Being strongly advised to define the url with the respective protocol. You must encode the originalURL with the percent-encoding (URL encoding).| [versionHistory=publico.pt](https://arquivo.pt/textsearch?prettyPrint=true&versionHistory=publico.pt) <br>OR<br>[versionHistory=http%3A%2F%2Fwww.imooty.pt/<br/>%2FOpiniao.php%3Fsort/<br/>%3Dlanguage%26action%3Dsort](https://arquivo.pt/textsearch?versionHistory=http%3A%2F%2Fwww.imooty.pt%2FOpiniao.php%3Fsort%3Dlanguage%26action%3Dsort&prettyPrint=true)|
 
# Response fields

| **Type of Search** | **Field Name** | **Description** | **Examples** |
|:------|:------|:-----------------------------|:--------|
| [Full-text](#search-for-terms), [URL](#search-for-url), [Metadata](#search-for-metadata) | serviceName &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;| Service Name. &nbsp; &nbsp;| "serviceName": "Arquivo.pt - the Portuguese web-archive"|
| [Full-text](#search-for-terms), [URL](#search-for-url), [Metadata](#search-for-metadata) | linkToService | URL for the service, in the case arquivo.pt | "linkToService": "https://arquivo.pt" |
| [Full-text](#search-for-terms), [URL](#search-for-url), [Metadata](#search-for-metadata) | request_parameters | field with all request parameters.| "request_parameters": <br>{<br>&nbsp;&nbsp;"q": "Albert Einstein",<br>&nbsp;&nbsp; "offset": 10,<br>&nbsp;&nbsp; "limit": 5,<br>&nbsp;&nbsp; "itemsPerSite": 2,<br>&nbsp;&nbsp;"prettyPrint": "true"<br> } |
| [Full-text](#search-for-terms), [URL](#search-for-url)| next_page | URL for the next N items. <br> N = offset + limit.<br>(offset=0 and limit=50). Default=50 | "next_page":"[http&#58;//arquivo.pt/textsearch?q=hello%20world&offset=50](https://arquivo.pt/textsearch?q=hello%20world&offset=50)" |
| [Full-text](#search-for-terms), [URL](#search-for-url) | previous_page | URL for previous N items.<br> N = offset - limit.<br>(offset=0 and limit=50). Default=0. | "previous_page":"[http&#58;//arquivo.pt/text<br>search?q=hello%20world&offset=0](https://arquivo.pt/textsearch?q=hello%20world&offset=0)" |
| [Full-text](#search-for-terms), [URL](#search-for-url) | estimated_nr_results |  Estimated total number of items for the search, without paging.| "estimated_nr_results": "8654051" | 
| [URL](#search-for-url) | total_items | Total number of items for the search, without paging. | "total_number" : "900" |
| [Full-text](#search-for-terms), [URL](#search-for-url), [Metadata](#search-for-metadata) | title* | Attribute of the HTML \<title\> tag of the original version. | "title": "Antonio Costa" | 
| [Full-text](#search-for-terms), [URL](#search-for-url), [Metadata](#search-for-metadata) | originalURL* | Version URL of the preserved version content. | "originalURL": "[http&#58;//zeca.uminho.pt/~costa/](http://zeca.uminho.pt/~costa/)" |
| [Full-text](#search-for-terms), [URL](#search-for-url), [Metadata](#search-for-metadata) | linkToArchive* | URL of the preserved version in Arquivo.pt. | "linkToArchive": "[http&#58;//arquivo.pt/wayback<br>/19961013191640/http&#58;//zeca.umi<br>nho.pt/~costa/](https://arquivo.pt/wayback/19961013191640/http://zeca.uminho.pt/~costa/)"
| [Full-text](#search-for-terms), [URL](#search-for-url), [Metadata](#search-for-metadata) | tstamp* | UTC timestamp of when the page was crawled - **NOT the timestamp of when it was published** <br> YYYY<br>MM<br>DD<br>HH<br>MM<br>SS<br> | "tstamp": "19961013191640" |
| [Full-text](#search-for-terms), [URL](#search-for-url), [Metadata](#search-for-metadata) | contentLength* | Size in bytes of the preserved version. | "contentLength": "1023" |
| [Full-text](#search-for-terms), [URL](#search-for-url), [Metadata](#search-for-metadata) | digest* | Version hash preserved.<br>Algorithm: MD5. | "digest": "5e8de36a1d6a76677a262e3e71d5c53d" |
| [Full-text](#search-for-terms), [URL](#search-for-url), [Metadata](#search-for-metadata) | mimeType* | MIME type of the character set. | "mimeType": "text" |
| [Full-text](#search-for-terms), [URL](#search-for-url), [Metadata](#search-for-metadata) | encoding* | Encoding of the content. <br> This field can return **empty**.  | "encoding": "windows-1252" |
| [Full-text](#search-for-terms), [URL](#search-for-url), [Metadata](#search-for-metadata) | date* | Date of crawl of the version.<br> Format: epoch | "date": "0845224210" | 
| [Full-text](#search-for-terms), [URL](#search-for-url), [Metadata](#search-for-metadata) | linkToScreenshot* | URL to download in image format.| "linkToScreenshot": "[http&#58;//arquivo.pt/screenshot/?<br>url=http%3A%2F%2Farquivo.pt%2Fno<br>Frame%2Freplay%2F199610<br>13191640%2Fhttp%3A%2F%2Fzeca.<br>uminho.pt%2F%7Ecosta%2F](https://arquivo.pt/screenshot/?url=http%3A%2F%2Farquivo.pt%2FnoFrame%2Freplay%2F19961013191640%2Fhttp%3A%2F%2Fzeca.uminho.pt%2F~costa%2F)"|
| [Full-text](#search-for-terms), [URL](#search-for-url), [Metadata](#search-for-metadata) | linkToNoFrame* | Link to the reproduction of the item, without the identifying sidebars of the [Arquivo.pt](http://www.arquivo.pt). | "linkToNoFrame": "[https://arquivo.pt/noFrame/rep<br>lay/20010919060531/http://www.ex<br>presso.pt/](https://arquivo.pt/noFrame/replay/20010919060531/http://www.expresso.pt/)" |
| [Full-text](#search-for-terms), [URL](#search-for-url), [Metadata](#search-for-metadata) | linkToOriginalFile* | Link to the original HTML of the preserved version.  | "linkToOriginalFile": "[https://arquivo.pt/noFrame/rep<br>lay/20010919060531id_/http://www.ex<br>presso.pt/](https://arquivo.pt/noFrame/replay/20010919060531id_/http://www.expresso.pt/)"|
| [Full-text](#search-for-terms), [URL](#search-for-url), [Metadata](#search-for-metadata) | linkToExtractedText* | URL for file download with filtered text extracted from preserved version. <br> This field can return **empty**.  | "linkToExtractedText": "[https://arquivo.pt/texte<br>xtracted?m=http%3A%2F%2Fwww.exp<br>resso.pt%2F%2F20010628221949](https://arquivo.pt/textextracted?m=http%3A%2F%2Fwww.expresso.pt%2F%2F20010628221949)"|
| [Full-text](#search-for-terms), [URL](#search-for-url)| linkToMetadata* | Link to web document metadata. | "linkToMetadata": "[https://arquivo.pt/textse<br>arch?metadata=http%3A%2F%2Fsapo.p<br>t%2Fhomepages%2Fm%2Fmarcelo<br>%2Findex.html%2F19991023104844](https://arquivo.pt/textsearch?metadata=http%3A%2F%2Fsapo.pt%2Fhomepages%2Fm%2Fmarcelo%2Findex.html%2F19991023104844)" | 
| [Full-text](#search-for-terms) | snippet* | Snippet is a block of text extracted from preserved web content that contains matches with search terms. HTML format, same as snippet displayed on [Arquivo.pt](http://www.arquivo.pt) search page. | "snippet": "\<em\>Antonio\</em\> \<em\>Costa\</em\><br> Ant&oacute;nio \<em\>Costa\</em\> Pe<br>rsonal Information Position: Lecturer, Computer Communications Group<br>\<span class=\"ellipsis\"\><br> ... \</span\> Address: \<em\>costa\</em\>@uminho.pt X.500 Information Ent<br>ry . Curriculum vitae Ant&oacute;n<br>io \<em\>Costa\</em\> , Page last modified: Fri Ju<br>n 16 19:16:13 MET DST 1995 Internet URL-<br> http&#58;/zeca.umi<br>nho.pt:80/~\<em\>costa\</em\>/\<span cla<br>ss=\"ellipsis\"\> ... \</s<br>pan\>" |
| [URL](#search-for-url), [Metadata](#search-for-metadata) | statusCode* | Crawl request HTTP status-code to the preserved version. | "statusCode": "200" | The HTTP request status code of the crawl for the preserved URL. |
| [Full-text](#search-for-terms), [URL](#search-for-url), [Metadata](#search-for-metadata) | collection* | The crawlings of the preserved Web in [Arquivo.pt](http://www.arquivo.pt) are divided by collections. This field returns the collection identifier of the preserved version.<br> This field can return **empty**. | "collection": "AWP3" |
| [Metadata](#search-for-metadata) | filename | Name of the ARC file where the item was extracted. <br> This field can return **empty**.| "filename": "IAH-20110425150147-00016-p12.arquivo.pt.arc.gz" |
| [Metadata](#search-for-metadata) | offset | The position of the CDX indices where the item is.<br> This field can return **empty**.| "offset": "9537529" |


*Response fields are repeated N times. Where N is the number of response items.

# Examples Requests

## Example Request - Full-text Search
Full-text Search for the terms Albert and Einstein, with a limit of 5 results, with the offset to 10. Finally, returns the results with formatting.

https://arquivo.pt/textsearch?q=Albert%20Einstein&maxItems=5&prettyPrint=true

## Example Response - Full-text Search

```
{
  "serviceName": "Arquivo.pt - the Portuguese web-archive",
  "linkToService": "https://arquivo.pt",
  "next_page": "https://arquivo.pt/textsearch?q=Albert%20Einstein&maxItems=5&prettyPrint=true&offset=5",
  "previous_page": "https://arquivo.pt/textsearch?q=Albert%20Einstein&maxItems=5&prettyPrint=true&offset=0",
  "request_parameters": {
    "q": "Albert Einstein",
    "maxItems": "5",
    "prettyPrint": "true"
  },
  "response_items": [
    {
      "title": "Albert Einstein ? Wikipédia, a enciclopédia livre",
      "originalURL": "http://pt.wikipedia.org/wiki/Albert_Einstein",
      "linkToArchive": "https://arquivo.pt/wayback/20150408223215/http://pt.wikipedia.org/wiki/Albert_Einstein",
      "tstamp": "20150408223215",
      "contentLength": "513829",
      "digest": "3bf5a50a6ec8adddc5225569d360123b",
      "mimeType": "text/html",
      "linkToScreenshot": "https://arquivo.pt/screenshot/?url=https%3A%2F%2Farquivo.pt%2FnoFrame%2Freplay%2F20150408223215%2Fhttp%3A%2F%2Fpt.wikipedia.org%2Fwiki%2FAlbert_Einstein",
      "date": "1428532335",
      "encoding": "UTF-8",
      "linkToNoFrame": "https://arquivo.pt/noFrame/replay/20150408223215/http://pt.wikipedia.org/wiki/Albert_Einstein",
      "linkToOriginalFile": "https://arquivo.pt/noFrame/replay/20150408223215id_/http://pt.wikipedia.org/wiki/Albert_Einstein",
      "snippet": "<em>Albert</em> <em>Einstein</em> &#8211; Wikip&eacute;dia, a enciclop&eacute;dia livre <em>Albert</em> <em>Einstein</em> Origem: Wikip&eacute;dia, a enciclop&eacute;dia livre. Ir para: navega&ccedil;&atilde;o , pesquisa Nota: &nbsp;Para outras acep&ccedil;&otilde;es do nome ver <em>Albert</em> <em>Einstein</em> (desambigua&ccedil;&atilde;o) e <em>Einstein</em> (desambigua&ccedil;&atilde;o) . <em>Albert</em> <em>Einstein</em>&nbsp; F&iacute;sica <em>Albert</em> <em>Einstein</em> em 1921 Dados gerais<span class=\"ellipsis\"> ... </span>",
      "collection": "AWP17",
      "linkToExtractedText": "https://arquivo.pt/textextracted?m=http%3A%2F%2Fpt.wikipedia.org%2Fwiki%2FAlbert_Einstein%2F20150408223215",
      "linkToMetadata": "https://arquivo.pt/textsearch?metadata=http%3A%2F%2Fpt.wikipedia.org%2Fwiki%2FAlbert_Einstein%2F20150408223215"
    },
    {
      "title": "Albert Einstein - Wikipedia, the free encyclopedia",
      "originalURL": "http://en.wikipedia.org/wiki/Albert_Einstein",
      "linkToArchive": "https://arquivo.pt/wayback/20150409010320/http://en.wikipedia.org/wiki/Albert_Einstein",
      "tstamp": "20150409010320",
      "contentLength": "498243",
      "digest": "140922258a89e6d639189f10ffaf5f1a",
      "mimeType": "text/html",
      "linkToScreenshot": "https://arquivo.pt/screenshot/?url=https%3A%2F%2Farquivo.pt%2FnoFrame%2Freplay%2F20150409010320%2Fhttp%3A%2F%2Fen.wikipedia.org%2Fwiki%2FAlbert_Einstein",
      "date": "1428541400",
      "encoding": "UTF-8",
      "linkToNoFrame": "https://arquivo.pt/noFrame/replay/20150409010320/http://en.wikipedia.org/wiki/Albert_Einstein",
      "linkToOriginalFile": "https://arquivo.pt/noFrame/replay/20150409010320id_/http://en.wikipedia.org/wiki/Albert_Einstein",
      "snippet": "<em>Albert</em> <em>Einstein</em> - Wikipedia, the free encyclopedia <em>Albert</em> <em>Einstein</em> From Wikipedia, the free encyclopedia Jump to: navigation , search &quot;<em>Einstein</em>&quot; redirects here. For other uses, see <em>Albert</em> <em>Einstein</em> (disambiguation) and <em>Einstein</em> (disambiguation) . <em>Albert</em> <em>Einstein</em> <em>Albert</em> <em>Einstein</em> in 1921 Born ( 1879<span class=\"ellipsis\"> ... </span>",
      "collection": "AWP17",
      "linkToExtractedText": "https://arquivo.pt/textextracted?m=http%3A%2F%2Fen.wikipedia.org%2Fwiki%2FAlbert_Einstein%2F20150409010320",
      "linkToMetadata": "https://arquivo.pt/textsearch?metadata=http%3A%2F%2Fen.wikipedia.org%2Fwiki%2FAlbert_Einstein%2F20150409010320"
    },
    {
      "title": "Albert Einstein ? Wikipédia, a enciclopédia livre",
      "originalURL": "http://pt.wikipedia.org/wiki/Albert_Einstein",
      "linkToArchive": "https://arquivo.pt/wayback/20120122174556/http://pt.wikipedia.org/wiki/Albert_Einstein",
      "tstamp": "20120122174556",
      "contentLength": "323194",
      "digest": "a57bdfdb64905ce26c777f59a20b04b0",
      "mimeType": "text/html",
      "linkToScreenshot": "https://arquivo.pt/screenshot/?url=https%3A%2F%2Farquivo.pt%2FnoFrame%2Freplay%2F20120122174556%2Fhttp%3A%2F%2Fpt.wikipedia.org%2Fwiki%2FAlbert_Einstein",
      "date": "1327254356",
      "encoding": "UTF-8",
      "linkToNoFrame": "https://arquivo.pt/noFrame/replay/20120122174556/http://pt.wikipedia.org/wiki/Albert_Einstein",
      "linkToOriginalFile": "https://arquivo.pt/noFrame/replay/20120122174556id_/http://pt.wikipedia.org/wiki/Albert_Einstein",
      "snippet": "<em>Albert</em> <em>Einstein</em> &#8211; Wikip&eacute;dia, a enciclop&eacute;dia livre <em>Albert</em> <em>Einstein</em> Origem: Wikip&eacute;dia, a enciclop&eacute;dia livre. Ir para: navega&ccedil;&atilde;o , pesquisa &nbsp; Nota: &nbsp; Para outros significados, veja <em>Albert</em> <em>Einstein</em><span class=\"ellipsis\"> ... </span> <em>Einstein</em> (desambigua&ccedil;&atilde;o) . <em>Albert</em> <em>Einstein</em>&nbsp; F&iacute;sica <em>Albert</em> <em>Einstein</em>, em 1921 Nacionalidade Alem&atilde; (1879<span class=\"ellipsis\"> ... </span>",
      "collection": "AWP12",
      "linkToExtractedText": "https://arquivo.pt/textextracted?m=http%3A%2F%2Fpt.wikipedia.org%2Fwiki%2FAlbert_Einstein%2F20120122174556",
      "linkToMetadata": "https://arquivo.pt/textsearch?metadata=http%3A%2F%2Fpt.wikipedia.org%2Fwiki%2FAlbert_Einstein%2F20120122174556"
    },
    {
      "title": "Albert Einstein Online",
      "originalURL": "http://www.westegg.com/einstein/",
      "linkToArchive": "https://arquivo.pt/wayback/20090714011510/http://www.westegg.com/einstein/",
      "tstamp": "20090714011510",
      "contentLength": "17447",
      "digest": "6a6378ae90fe70a685a85a4b293c260c",
      "mimeType": "text/html",
      "linkToScreenshot": "https://arquivo.pt/screenshot/?url=https%3A%2F%2Farquivo.pt%2FnoFrame%2Freplay%2F20090714011510%2Fhttp%3A%2F%2Fwww.westegg.com%2Feinstein%2F",
      "date": "1247534110",
      "encoding": "windows-1252",
      "linkToNoFrame": "https://arquivo.pt/noFrame/replay/20090714011510/http://www.westegg.com/einstein/",
      "linkToOriginalFile": "https://arquivo.pt/noFrame/replay/20090714011510id_/http://www.westegg.com/einstein/",
      "snippet": "<em>Albert</em> <em>Einstein</em> Online [Overviews] Ten Obscure Factoids Concerning <em>Albert</em> <em>Einstein</em> <em>Albert</em> <em>Einstein</em> Biography <em>Albert</em> <em>Einstein</em> Biography , Nobelprize.org <em>Einstein</em>-Image and Impact . AIP History Center exhibit <em>Albert</em> <em>Einstein</em>'s Scientific Works Time Line of <em>Einstein</em>'s Life <em>Einstein</em>'s Big idea , Nova<span class=\"ellipsis\"> ... </span>",
      "collection": "AWP4",
      "linkToExtractedText": "https://arquivo.pt/textextracted?m=http%3A%2F%2Fwww.westegg.com%2Feinstein%2F%2F20090714011510",
      "linkToMetadata": "https://arquivo.pt/textsearch?metadata=http%3A%2F%2Fwww.westegg.com%2Feinstein%2F%2F20090714011510"
    },
    {
      "title": "Albert Einstein",
      "originalURL": "http://alberteinsteinemc2.blogspot.com/",
      "linkToArchive": "https://arquivo.pt/wayback/20091005062521/http://alberteinsteinemc2.blogspot.com/",
      "tstamp": "20091005062521",
      "contentLength": "122399",
      "digest": "146b4c57771907283fb3222efbea3ac7",
      "mimeType": "text/html",
      "linkToScreenshot": "https://arquivo.pt/screenshot/?url=https%3A%2F%2Farquivo.pt%2FnoFrame%2Freplay%2F20091005062521%2Fhttp%3A%2F%2Falberteinsteinemc2.blogspot.com%2F",
      "date": "1254723921",
      "encoding": "UTF-8",
      "linkToNoFrame": "https://arquivo.pt/noFrame/replay/20091005062521/http://alberteinsteinemc2.blogspot.com/",
      "linkToOriginalFile": "https://arquivo.pt/noFrame/replay/20091005062521id_/http://alberteinsteinemc2.blogspot.com/",
      "snippet": "<em>Albert</em> <em>Einstein</em> skip to main | skip to sidebar <em>Albert</em> <em>Einstein</em> <em>ALBERT</em> <em>EINSTEIN</em>, BIBLIGRAFIA, CARTAS, HIST&Oacute;RIA E TEORIAS <em>ALBERT</em> <em>EINSTEIN</em> - ANIMA&Ccedil;&Atilde;O <em>Albert</em> <em>Einstein</em>, Textos: Video <em>ALBERT</em> <em>EINSTEIN</em> - BIOGRAFIA <em>Albert</em> <em>Einstein</em> (Ulm, 14 de Mar&ccedil;o de 1879 - Princeton, 18 de Abril de 1955) foi um f&iacute;sico<span class=\"ellipsis\"> ... </span>",
      "collection": "AWP5",
      "linkToExtractedText": "https://arquivo.pt/textextracted?m=http%3A%2F%2Falberteinsteinemc2.blogspot.com%2F%2F20091005062521",
      "linkToMetadata": "https://arquivo.pt/textsearch?metadata=http%3A%2F%2Falberteinsteinemc2.blogspot.com%2F%2F20091005062521"
    }
  ]
}
```

## Example Request - URL Search

URL Search for the *expresso.pt* versions, with a limit of 5 results. Finally, returns the results with formatting.

https://arquivo.pt/textsearch?prettyPrint=true&versionHistory=expresso.pt&maxItems=5


## Example Response - URL Search
```
{
  "serviceName": "Arquivo.pt - the Portuguese web-archive",
  "linkToService": "https://arquivo.pt",
  "next_page": "https://arquivo.pt/textsearch?prettyPrint=true&versionHistory=expresso.pt&maxItems=5&offset=5",
  "previous_page": "https://arquivo.pt/textsearch?prettyPrint=true&versionHistory=expresso.pt&maxItems=5&offset=0",
  "request_parameters": {
    "maxItems": "5",
    "from": "19960101000000",
    "to": "20181231235959",
    "prettyPrint": "true"
  },
  "response_items": [
    {
      "title": "302 Found",
      "originalURL": "http://www.expresso.pt/",
      "linkToArchive": "https://arquivo.pt/wayback/20160719170305/http://www.expresso.pt/",
      "tstamp": "20160719170305",
      "contentLength": "318",
      "digest": "POPLDWWERZ2PUX2BRPCFNS2BB6ELQHMY",
      "mimeType": "text/html",
      "linkToScreenshot": "https://arquivo.pt/screenshot/?url=https%3A%2F%2Farquivo.pt%2FnoFrame%2Freplay%2F20160719170305%2Fhttp%3A%2F%2Fwww.expresso.pt%2F",
      "date": "1468947785",
      "encoding": "iso-8859-1",
      "linkToNoFrame": "https://arquivo.pt/noFrame/replay/20160719170305/http://www.expresso.pt/",
      "linkToOriginalFile": "https://arquivo.pt/noFrame/replay/20160719170305id_/http://www.expresso.pt/",
      "status": "302",
      "collection": "FAWP2620160719",
      "linkToExtractedText": "https://arquivo.pt/textextracted?m=http%3A%2F%2Fwww.expresso.pt%2F%2F20160719170305",
      "linkToMetadata": "https://arquivo.pt/textsearch?metadata=http%3A%2F%2Fwww.expresso.pt%2F%2F20160719170305"
    },
    {
      "title": "302 Found",
      "originalURL": "http://www.expresso.pt/",
      "linkToArchive": "https://arquivo.pt/wayback/20160718170306/http://www.expresso.pt/",
      "tstamp": "20160718170306",
      "contentLength": "316",
      "digest": "POPLDWWERZ2PUX2BRPCFNS2BB6ELQHMY",
      "mimeType": "text/html",
      "linkToScreenshot": "https://arquivo.pt/screenshot/?url=https%3A%2F%2Farquivo.pt%2FnoFrame%2Freplay%2F20160718170306%2Fhttp%3A%2F%2Fwww.expresso.pt%2F",
      "date": "1468861386",
      "encoding": "iso-8859-1",
      "linkToNoFrame": "https://arquivo.pt/noFrame/replay/20160718170306/http://www.expresso.pt/",
      "linkToOriginalFile": "https://arquivo.pt/noFrame/replay/20160718170306id_/http://www.expresso.pt/",
      "status": "302",
      "collection": "FAWP2620160718",
      "linkToExtractedText": "https://arquivo.pt/textextracted?m=http%3A%2F%2Fwww.expresso.pt%2F%2F20160718170306",
      "linkToMetadata": "https://arquivo.pt/textsearch?metadata=http%3A%2F%2Fwww.expresso.pt%2F%2F20160718170306"
    },
    {
      "title": "302 Found",
      "originalURL": "http://www.expresso.pt/",
      "linkToArchive": "https://arquivo.pt/wayback/20160717170308/http://www.expresso.pt/",
      "tstamp": "20160717170308",
      "contentLength": "318",
      "digest": "POPLDWWERZ2PUX2BRPCFNS2BB6ELQHMY",
      "mimeType": "text/html",
      "linkToScreenshot": "https://arquivo.pt/screenshot/?url=https%3A%2F%2Farquivo.pt%2FnoFrame%2Freplay%2F20160717170308%2Fhttp%3A%2F%2Fwww.expresso.pt%2F",
      "date": "1468774988",
      "encoding": "iso-8859-1",
      "linkToNoFrame": "https://arquivo.pt/noFrame/replay/20160717170308/http://www.expresso.pt/",
      "linkToOriginalFile": "https://arquivo.pt/noFrame/replay/20160717170308id_/http://www.expresso.pt/",
      "status": "302",
      "collection": "FAWP2620160717",
      "linkToExtractedText": "https://arquivo.pt/textextracted?m=http%3A%2F%2Fwww.expresso.pt%2F%2F20160717170308",
      "linkToMetadata": "https://arquivo.pt/textsearch?metadata=http%3A%2F%2Fwww.expresso.pt%2F%2F20160717170308"
    },
    {
      "title": "302 Found",
      "originalURL": "http://www.expresso.pt/",
      "linkToArchive": "https://arquivo.pt/wayback/20160716170306/http://www.expresso.pt/",
      "tstamp": "20160716170306",
      "contentLength": "317",
      "digest": "POPLDWWERZ2PUX2BRPCFNS2BB6ELQHMY",
      "mimeType": "text/html",
      "linkToScreenshot": "https://arquivo.pt/screenshot/?url=https%3A%2F%2Farquivo.pt%2FnoFrame%2Freplay%2F20160716170306%2Fhttp%3A%2F%2Fwww.expresso.pt%2F",
      "date": "1468688586",
      "encoding": "iso-8859-1",
      "linkToNoFrame": "https://arquivo.pt/noFrame/replay/20160716170306/http://www.expresso.pt/",
      "linkToOriginalFile": "https://arquivo.pt/noFrame/replay/20160716170306id_/http://www.expresso.pt/",
      "status": "302",
      "collection": "FAWP2620160716",
      "linkToExtractedText": "https://arquivo.pt/textextracted?m=http%3A%2F%2Fwww.expresso.pt%2F%2F20160716170306",
      "linkToMetadata": "https://arquivo.pt/textsearch?metadata=http%3A%2F%2Fwww.expresso.pt%2F%2F20160716170306"
    },
    {
      "title": "302 Found",
      "originalURL": "http://www.expresso.pt/",
      "linkToArchive": "https://arquivo.pt/wayback/20160715170308/http://www.expresso.pt/",
      "tstamp": "20160715170308",
      "contentLength": "317",
      "digest": "POPLDWWERZ2PUX2BRPCFNS2BB6ELQHMY",
      "mimeType": "text/html",
      "linkToScreenshot": "https://arquivo.pt/screenshot/?url=https%3A%2F%2Farquivo.pt%2FnoFrame%2Freplay%2F20160715170308%2Fhttp%3A%2F%2Fwww.expresso.pt%2F",
      "date": "1468602188",
      "encoding": "iso-8859-1",
      "linkToNoFrame": "https://arquivo.pt/noFrame/replay/20160715170308/http://www.expresso.pt/",
      "linkToOriginalFile": "https://arquivo.pt/noFrame/replay/20160715170308id_/http://www.expresso.pt/",
      "status": "302",
      "collection": "FAWP2620160715",
      "linkToExtractedText": "https://arquivo.pt/textextracted?m=http%3A%2F%2Fwww.expresso.pt%2F%2F20160715170308",
      "linkToMetadata": "https://arquivo.pt/textsearch?metadata=http%3A%2F%2Fwww.expresso.pt%2F%2F20160715170308"
    }
  ]
}
```


## Example Response - Metadata Search

Metadata search for the *expresso.pt* version, with *20000302151731* timestamp. Finally, returns the results with formatting.

Metadata Link:
https://arquivo.pt/textsearch?metadata=http%3A%2F%2Fwww.expresso.pt%2F%2F20000302151731&prettyPrint=true


```
{
  "serviceName": "Arquivo.pt - the Portuguese web-archive",
  "linkToService": "https://arquivo.pt",
  "request_parameters": {
    "prettyPrint": "true"
  },
  "response_items": [
    {
      "title": "EXPRESSO",
      "originalURL": "http://www.expresso.pt/",
      "linkToArchive": "https://arquivo.pt/wayback/20000302151731/http://www.expresso.pt/",
      "tstamp": "20000302151731",
      "contentLength": "33905",
      "digest": "IGLEXSO4SFEQJD5NGIZZTQVLXHEPM5GS",
      "mimeType": "text/html",
      "linkToScreenshot": "https://arquivo.pt/screenshot/?url=http%3A%2F%2Farquivo.pt%2FnoFrame%2Freplay%2F20000302151731%2Fhttp%3A%2F%2Fwww.expresso.pt%2F",
      "date": "0952010251",
      "encoding": "windows-1252",
      "linkToNoFrame": "https://arquivo.pt/noFrame/replay/20000302151731/http://www.expresso.pt/",
      "linkToOriginalFile": "https://arquivo.pt/noFrame/replay/20000302151731id_/http://www.expresso.pt/",
      "status": "200",
      "collection": "IA20000302",
      "linkToExtractedText": "https://arquivo.pt/textextracted?m=http%3A%2F%2Fwww.expresso.pt%2F%2F20000302151731",
      "linkToMetadata": "https://arquivo.pt/textsearch?metadata=http%3A%2F%2Fwww.expresso.pt%2F%2F20000302151731",
      "filename": "PT-HISTORICAL-2000-GROUP-ALV-20100830000000-00000.arc.gz",
      "offset": "11319239"
    }
  ]
}
```

## REST from JavaScript
You can invoke the JSON TextSearch API using REST from JavaScript, using the callback query parameter and a callback function. This allows you to write rich applications that display Custom Search data without writing any server side code.

The following example uses this approach to display the first page of search results for the query _Simone de Beauvoir_:

```
<html>
  <head>
    <title>JSON TextSearch API Example</title>
  </head>
  <body>
    <div id="content"></div>
    <script>
    function hndlr(response) {

      	for (var i = 0; i < response.response_items.length; i++) {
        	var item = response.response_items[i];
        	// in production code, item.htmlTitle should have the HTML entities escaped.
        	document.getElementById("content").innerHTML += "<br>" + "<strong>Page Title</strong>: " + item.title + " <strong>URL</strong>: " + item.linkToArchive;
      	}
    }
    </script>
    <script src="https://arquivo.pt/textsearch?q=simone%20de%20beauvoir&from=19960101000000&to=20001022163016&maxItems=10&offset=0&callback=hndlr">
    </script>
  </body>
</html>
```


## Client API JAVA

[https://github.com/arquivo/client-java-api](https://github.com/arquivo/client-java-api)