package service

import (
	"net/http"
	"strings"

	docsPkg "github.com/onexay/kv-vs/docs"
)

const swaggerHTML = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>kv-vs API</title>
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css">
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
  <script>
    window.onload = function() {
      SwaggerUIBundle({
        url: 'openapi.yaml',
        dom_id: '#swagger-ui',
        deepLinking: true,
      });
    };
  </script>
</body>
</html>`

func (s *Service) handleSwagger(w http.ResponseWriter, r *http.Request, tail string) {
	tail = strings.TrimPrefix(tail, "/")
	switch tail {
	case "", "index.html":
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(swaggerHTML))
	case "openapi.yaml", "openapi.yml", "openapi.json":
		w.Header().Set("Content-Type", "application/yaml")
		_, _ = w.Write(docsPkg.OpenAPI)
	default:
		http.NotFound(w, r)
	}
}
