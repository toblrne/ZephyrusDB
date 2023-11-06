package api

import (
	"net/http"
	"github.com/toblrne/ZephyrusDBv2/db"
	"github.com/gin-gonic/gin"
)

type Handler struct {
	driver *db.Driver
}

func NewHandler(driver *db.Driver) *Handler {
	return &Handler{driver: driver}
}

func (h *Handler) PutValue(c *gin.Context) {
	key := c.Param("key")

	// Read the content type of the incoming request
	contentType := c.GetHeader("Content-Type")

	var value []byte
	var err error

	// If the content type is JSON, use the JSON binding
	if contentType == "application/json" {
		var jsonValue interface{}
		if err = c.BindJSON(&jsonValue); err == nil {
			value, err = db.MarshalJson(jsonValue)
		}
	} else {
		// For all other content types, read the body as raw bytes
		value, err = c.GetRawData()
	}

	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid value"})
		return
	}

	err = h.driver.Put(key, value)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.Status(http.StatusOK)
}

func (h *Handler) GetValue(c *gin.Context) {
	key := c.Param("key")
	value, err := h.driver.Get(key)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}

	// Respond with the content type that the value is stored in
	contentType := http.DetectContentType(value)
	if contentType == "application/json" {
		// If the content type is JSON, send it as JSON
		c.Data(http.StatusOK, contentType, value)
	} else {
		// Otherwise, send it as raw data
		c.Data(http.StatusOK, "application/octet-stream", value)
	}
}

func (h *Handler) DeleteValue(c *gin.Context) {
	key := c.Param("key")
	err := h.driver.Delete(key)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.Status(http.StatusOK)
}
