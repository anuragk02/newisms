package handlers

import (
	"github.com/anuragk02/newisms/newisms-server/internal/database"
	"net/http"
	"github.com/gin-gonic/gin"
)

type Handler struct {
	db *database.DB
}

func NewHandler(db *database.DB) *Handler {
	return &Handler{db: db}
}

func (h *Handler) HealthCheck(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

