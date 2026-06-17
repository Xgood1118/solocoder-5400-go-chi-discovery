package auth

import (
	"net/http"
)

type Authenticator struct {
	token string
}

func NewAuthenticator(token string) *Authenticator {
	return &Authenticator{token: token}
}

func (a *Authenticator) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if a.token == "" {
			next.ServeHTTP(w, r)
			return
		}

		reqToken := r.Header.Get("X-Registry-Token")
		if reqToken == "" || reqToken != a.token {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func (a *Authenticator) WriteMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet || r.Method == http.MethodHead {
			next.ServeHTTP(w, r)
			return
		}

		if a.token == "" {
			next.ServeHTTP(w, r)
			return
		}

		reqToken := r.Header.Get("X-Registry-Token")
		if reqToken == "" || reqToken != a.token {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		next.ServeHTTP(w, r)
	})
}
