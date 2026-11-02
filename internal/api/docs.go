package api

import (
	"crypto/sha256"
	_ "embed"
	"encoding/base64"
	"net/http"
)

var openAPISpec []byte

const (
	swaggerUIBase      = "https://cdn.jsdelivr.net/npm/swagger-ui-dist@5.17.14/"
	swaggerUIScriptSRI = "sha384-wmyclcVGX/WhUkdkATwhaK1X1JtiNrr2EoYJ+diV3vj4v6OC5yCeSu+yW13SYJep"
	swaggerUIStyleSRI  = "sha384-wxLW6kwyHktdDGr6Pv1zgm/VGJh99lfUbzSn6HNHBENZlCN7W602k9VkGdxuFvPn"
)

const docsInitScript = `window.onload = () => { SwaggerUIBundle({ url: "/openapi.json", dom_id: "#swagger-ui", validatorUrl: null }); };`

var docsPage = []byte(`<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>datapipe API</title>
<link rel="stylesheet" href="` + swaggerUIBase + `swagger-ui.css" integrity="` + swaggerUIStyleSRI + `" crossorigin="anonymous">
</head>
<body>
<div id="swagger-ui"></div>
<script src="` + swaggerUIBase + `swagger-ui-bundle.js" integrity="` + swaggerUIScriptSRI + `" crossorigin="anonymous"></script>
<script>` + docsInitScript + `</script>
</body>
</html>
`)

var docsCSP = "default-src 'none'; " +
	"script-src https://cdn.jsdelivr.net '" + scriptHash(docsInitScript) + "'; " +
	"style-src https://cdn.jsdelivr.net 'unsafe-inline'; " +
	"img-src 'self' data:; " +
	"connect-src 'self'; " +
	"base-uri 'none'; form-action 'none'; frame-ancestors 'none'"

func scriptHash(s string) string {
	sum := sha256.Sum256([]byte(s))
	return "sha256-" + base64.StdEncoding.EncodeToString(sum[:])
}

func handleOpenAPI(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(openAPISpec)
}

func handleDocs(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Content-Security-Policy", docsCSP)
	_, _ = w.Write(docsPage)
}
