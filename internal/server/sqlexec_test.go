package server

import (
	"net/http"
	"testing"
)

func TestExecSQLRequiresExplicitTargetRole(t *testing.T) {
	_, front := newTestServer(t)
	connection := createConnection(t, front, "db", 33091)

	status := statusOf(t, front, http.MethodPost, "/api/jobs/exec-sql", map[string]any{
		"connectionId": connection["id"],
		"content":      "DELETE FROM users;",
		"mode":         execModeDirect,
	})
	if status != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d when targetRole is missing", status, http.StatusBadRequest)
	}
}

func TestExecSQLValidatesExecuteOnAgainstTargetRole(t *testing.T) {
	_, front := newTestServer(t)
	connection := createConnection(t, front, "db", 33092)

	tests := []struct {
		name       string
		content    string
		targetRole string
		wantStatus int
	}{
		{
			name:       "matching source marker",
			content:    "-- DATASMITH EXECUTE-ON: source\nDELETE FROM users;",
			targetRole: "source",
			wantStatus: http.StatusOK,
		},
		{
			name:       "conflicting target role",
			content:    "-- DATASMITH EXECUTE-ON: source\nDELETE FROM users;",
			targetRole: "target",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid marker value",
			content:    "-- DATASMITH EXECUTE-ON: elsewhere\nDELETE FROM users;",
			targetRole: "source",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "empty marker value",
			content:    "-- DATASMITH EXECUTE-ON:\nDELETE FROM users;",
			targetRole: "source",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "unmarked custom SQL",
			content:    "DELETE FROM users;",
			targetRole: "source",
			wantStatus: http.StatusOK,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			status := statusOf(t, front, http.MethodPost, "/api/jobs/exec-sql", map[string]any{
				"connectionId": connection["id"],
				"content":      test.content,
				"mode":         execModeDirect,
				"targetRole":   test.targetRole,
			})
			if status != test.wantStatus {
				t.Fatalf("status = %d, want %d", status, test.wantStatus)
			}
		})
	}
}
