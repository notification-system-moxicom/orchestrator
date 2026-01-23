package handlers

import "net/http"

type Handlers struct {
}

func NewHandlers() *Handlers {
	return &Handlers{}
}

func (h *Handlers) GetSystems(w http.ResponseWriter, r *http.Request) {
	//TODO implement me
	panic("implement me")
}

func (h *Handlers) PostSystems(w http.ResponseWriter, r *http.Request) {
	//TODO implement me
	panic("implement me")
}

func (h *Handlers) Notify(w http.ResponseWriter, r *http.Request, systemId string) {
	//TODO implement me
	panic("implement me")
}

func (h *Handlers) GetUsers(w http.ResponseWriter, r *http.Request, systemId string) {
	//TODO implement me
	panic("implement me")
}

func (h *Handlers) AddUser(w http.ResponseWriter, r *http.Request, systemId string) {
	//TODO implement me
	panic("implement me")
}
