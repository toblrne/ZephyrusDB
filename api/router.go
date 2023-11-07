package api

import (
	"github.com/gin-gonic/gin"
)

// InitRouter initializes and returns the Gin Engine with configured routes
func InitRouter(handler *Handler) *gin.Engine {
	router := gin.Default()

	router.PUT("/key/:key", handler.PutValue)
	router.GET("/key/:key", handler.GetValue)
	router.DELETE("/key/:key", handler.DeleteValue)

	return router
}
