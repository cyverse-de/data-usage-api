package natsconn

import "time"

type UserDataUsage struct {
	ID           string    `db:"id" json:"id"`
	UserID       string    `db:"user_id" json:"user_id"`
	Username     string    `db:"username" json:"username"`
	Total        int64     `db:"total" json:"total"`
	Time         time.Time `db:"time" json:"time"`
	LastModified time.Time `db:"last_modified" json:"last_modified"`
}
