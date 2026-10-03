package api

import (
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/wyw14/cry-98/internal/fill"
)

type autoFillPolicyPayload struct {
	LowThreshold       float64 `json:"low_threshold"`
	RecoveryThreshold  float64 `json:"recovery_threshold"`
	ConsecutiveSamples int     `json:"consecutive_samples"`
	CooldownSeconds    float64 `json:"cooldown_seconds"`
}

func (p autoFillPolicyPayload) toPolicy() fill.AutoPolicy {
	return fill.AutoPolicy{
		LowThreshold:       p.LowThreshold,
		RecoveryThreshold:  p.RecoveryThreshold,
		ConsecutiveSamples: p.ConsecutiveSamples,
		Cooldown:           time.Duration(p.CooldownSeconds * float64(time.Second)),
	}
}

func autoFillPolicyPayloadFrom(policy fill.AutoPolicy) autoFillPolicyPayload {
	return autoFillPolicyPayload{
		LowThreshold:       policy.LowThreshold,
		RecoveryThreshold:  policy.RecoveryThreshold,
		ConsecutiveSamples: policy.ConsecutiveSamples,
		CooldownSeconds:    policy.Cooldown.Seconds(),
	}
}

func (s *Server) getAutoFillPolicy(writer http.ResponseWriter, request *http.Request) {
	vesselID := chi.URLParam(request, "id")
	policy, ok := s.deps.AutoFill.Policy(vesselID)
	if !ok {
		writeError(writer, http.StatusNotFound, "auto-fill policy is not configured for vessel")
		return
	}
	writeJSON(writer, http.StatusOK, autoFillPolicyPayloadFrom(policy))
}

func (s *Server) putAutoFillPolicy(writer http.ResponseWriter, request *http.Request) {
	vesselID := chi.URLParam(request, "id")
	if _, ok := s.deps.Vessels.Get(vesselID); !ok {
		writeError(writer, http.StatusNotFound, "vessel not found")
		return
	}
	var input autoFillPolicyPayload
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	policy := input.toPolicy()
	if err := s.deps.AutoFill.SetPolicy(vesselID, policy); err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(writer, http.StatusOK, autoFillPolicyPayloadFrom(policy))
}

func (s *Server) deleteAutoFillPolicy(writer http.ResponseWriter, request *http.Request) {
	vesselID := chi.URLParam(request, "id")
	s.deps.AutoFill.RemovePolicy(vesselID)
	writeJSON(writer, http.StatusOK, map[string]string{"status": "disabled"})
}
