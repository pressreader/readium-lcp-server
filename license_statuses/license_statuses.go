// Copyright 2020 Readium Foundation. All rights reserved.
// Use of this source code is governed by a BSD-style license
// that can be found in the LICENSE file exposed on Github (readium) in the project repository.

package licensestatuses

import (
	"database/sql"
	"errors"
	"log"
	"strings"
	"time"

	"github.com/readium/readium-lcp-server/config"
	"github.com/readium/readium-lcp-server/status"
)

// ErrNotFound is license status not found
var ErrNotFound = errors.New("License Status not found")

// LicenseStatuses is an interface
type LicenseStatuses interface {
	getByID(id int) (*LicenseStatus, error)
	Add(ls LicenseStatus) error
	List(deviceLimit int64, limit int64, offset int64) func() (LicenseStatus, error)
	GetByLicenseID(id string) (*LicenseStatus, error)
	Update(ls LicenseStatus) error
}

type dbLicenseStatuses struct {
	db             *sql.DB
	get            *sql.Stmt
	add            *sql.Stmt
	list           *sql.Stmt
	getbylicenseid *sql.Stmt
	update         *sql.Stmt
}

//Get gets license status by id
//
// Removed in 94722fcb4a0a38bd5f765e67b0538f2042192ac4 but breaks the test,
// so putting it back as unexported.
// FIXME: check if could be useful; delete if not, be careful about tests
func (i dbLicenseStatuses) getByID(id int) (*LicenseStatus, error) {
	var statusDB int64
	ls := LicenseStatus{}

	var potentialRightsEnd *time.Time
	var licenseUpdate *time.Time
	var statusUpdate *time.Time

	row := i.get.QueryRow(id)
	err := row.Scan(&ls.ID, &statusDB, &licenseUpdate, &statusUpdate, &ls.DeviceCount, &potentialRightsEnd, &ls.LicenseRef, &ls.CurrentEndLicense)

	if err == nil {
		status.GetStatus(statusDB, &ls.Status)

		ls.Updated = new(Updated)

		if (potentialRightsEnd != nil) && (!(*potentialRightsEnd).IsZero()) {
			ls.PotentialRights = new(PotentialRights)
			ls.PotentialRights.End = potentialRightsEnd
		}

		if licenseUpdate != nil || statusUpdate != nil {
			ls.Updated.Status = statusUpdate
			ls.Updated.License = licenseUpdate
		}
	} else {
		if err == sql.ErrNoRows {
			return nil, ErrNotFound
		}
	}

	return &ls, err
}

//Add adds license status to database
func (i dbLicenseStatuses) Add(ls LicenseStatus) error {
	statusDB, err := status.SetStatus(ls.Status)

	if err == nil {
		var end *time.Time
		end = nil
		if ls.PotentialRights != nil && ls.PotentialRights.End != nil && !(*ls.PotentialRights.End).IsZero() {
			end = ls.PotentialRights.End
		}
		_, err = i.add.Exec(statusDB, ls.Updated.License, ls.Updated.Status, ls.DeviceCount, end, ls.LicenseRef, ls.CurrentEndLicense)
	}

	return err
}

// List gets license statuses which have devices count more than devices limit
// input parameters: limit - how much license statuses need to get, offset - from what position need to start
func (i dbLicenseStatuses) List(deviceLimit int64, limit int64, offset int64) func() (LicenseStatus, error) {
	rows, err := i.list.Query(deviceLimit, limit, offset)
	if err != nil {
		return func() (LicenseStatus, error) { return LicenseStatus{}, err }
	}
	return func() (LicenseStatus, error) {
		var statusDB int64
		ls := LicenseStatus{}
		ls.Updated = new(Updated)

		var err error
		if rows.Next() {
			err = rows.Scan(&ls.ID, &statusDB, &ls.Updated.License, &ls.Updated.Status, &ls.DeviceCount, &ls.LicenseRef)

			if err == nil {
				status.GetStatus(statusDB, &ls.Status)
			}
		} else {
			rows.Close()
		}
		return ls, err
	}
}

// GetByLicenseID gets license status by license id (uuid)
func (i dbLicenseStatuses) GetByLicenseID(licenseID string) (*LicenseStatus, error) {
	var statusDB int64
	ls := LicenseStatus{}

	var potentialRightsEnd *time.Time
	var licenseUpdate *time.Time
	var statusUpdate *time.Time

	row := i.getbylicenseid.QueryRow(licenseID)
	err := row.Scan(&ls.ID, &statusDB, &licenseUpdate, &statusUpdate, &ls.DeviceCount, &potentialRightsEnd, &ls.LicenseRef, &ls.CurrentEndLicense)

	if err == nil {
		status.GetStatus(statusDB, &ls.Status)

		ls.Updated = new(Updated)

		if (potentialRightsEnd != nil) && (!(*potentialRightsEnd).IsZero()) {
			ls.PotentialRights = new(PotentialRights)
			ls.PotentialRights.End = potentialRightsEnd
		}

		if licenseUpdate != nil || statusUpdate != nil {
			ls.Updated.Status = statusUpdate
			ls.Updated.License = licenseUpdate
		}
	} else {
		if err == sql.ErrNoRows {
			return nil, err
		}
	}

	return &ls, err
}

// Update updates a license status
func (i dbLicenseStatuses) Update(ls LicenseStatus) error {

	statusInt, err := status.SetStatus(ls.Status)
	if err != nil {
		return err
	}

	var potentialRightsEnd *time.Time

	if ls.PotentialRights != nil && ls.PotentialRights.End != nil && !(*ls.PotentialRights.End).IsZero() {
		potentialRightsEnd = ls.PotentialRights.End
	}

	var result sql.Result
	result, err = i.update.Exec(statusInt, ls.Updated.License, ls.Updated.Status, ls.DeviceCount, potentialRightsEnd, ls.CurrentEndLicense, ls.ID)

	if err == nil {
		if r, _ := result.RowsAffected(); r == 0 {
			return ErrNotFound
		}
	}
	return err
}

// Open defines scripts for queries & create table license_status if it does not exist
func Open(db *sql.DB) (l LicenseStatuses, err error) {

	var createTableQuery, getQuery, getByLicenseIdQuery, addQuery, updateQuery, listQuery string
	if strings.HasPrefix(config.Config.LcpServer.Database, "postgres") {
		// postgres
		createTableQuery = tableDefPostgres
		getQuery = "SELECT * FROM license_status WHERE id = $1 LIMIT 1"
		getByLicenseIdQuery = "SELECT * FROM license_status where license_ref = $1"
		listQuery = "SELECT status, license_updated, status_updated, device_count, license_ref FROM license_status WHERE device_count >= $1 ORDER BY id DESC LIMIT $2 OFFSET $3"
		addQuery = "INSERT INTO license_status (status, license_updated, status_updated, device_count, potential_rights_end, license_ref, rights_end) VALUES ($1, $2, $3, $4, $5, $6, $7)"
		updateQuery = "UPDATE license_status SET status=$1, license_updated=$2, status_updated=$3, device_count=$4, potential_rights_end=$5, rights_end=$6 WHERE id=$7"
	} else {
		// mysql/sqlite
		createTableQuery = tableDef
		getQuery = "SELECT * FROM license_status WHERE id = ? LIMIT 1"
		getByLicenseIdQuery = "SELECT * FROM license_status where license_ref = ?"
		listQuery = "SELECT status, license_updated, status_updated, device_count, license_ref FROM license_status WHERE device_count >= ? ORDER BY id DESC LIMIT ? OFFSET ?"
		addQuery = "INSERT INTO license_status (status, license_updated, status_updated, device_count, potential_rights_end, license_ref, rights_end) VALUES (?, ?, ?, ?, ?, ?, ?)"
		updateQuery = "UPDATE license_status SET status=?, license_updated=?, status_updated=?, device_count=?,potential_rights_end=?,  rights_end=?  WHERE id=?"
	}

	// if sqlite/postgres, create the license_status table in the lsd db if it does not exist
	if strings.HasPrefix(config.Config.LsdServer.Database, "sqlite") || strings.HasPrefix(config.Config.LcpServer.Database, "postgres") {
		_, err = db.Exec(createTableQuery)
		if err != nil {
			log.Println("Error creating license_status table")
			return
		}
	}

	get, err := db.Prepare(getQuery)
	if err != nil {
		return
	}

	list, err := db.Prepare(listQuery)
	if err != nil {
		return
	}

	getbylicenseid, err := db.Prepare(getByLicenseIdQuery)
	if err != nil {
		return
	}

	add, err := db.Prepare(addQuery)
	if err != nil {
		return
	}

	update, err := db.Prepare(updateQuery)
	if err != nil {
		return
	}

	l = dbLicenseStatuses{db, get, add, list, getbylicenseid, update}
	return
}

const tableDef = "CREATE TABLE IF NOT EXISTS license_status (" +
	"id INTEGER PRIMARY KEY," +
	"status int(11) NOT NULL," +
	"license_updated datetime NOT NULL," +
	"status_updated datetime NOT NULL," +
	"device_count int(11) DEFAULT NULL," +
	"potential_rights_end datetime DEFAULT NULL," +
	"license_ref varchar(255) NOT NULL," +
	"rights_end datetime DEFAULT NULL  " +
	");" +
	"CREATE INDEX IF NOT EXISTS license_ref_index on license_status (license_ref);"

const tableDefPostgres = "CREATE TABLE IF NOT EXISTS license_status (" +
	"id SERIAL PRIMARY KEY," +
	"status INT NOT NULL," +
	"license_updated TIMESTAMPTZ NOT NULL," +
	"status_updated TIMESTAMPTZ NOT NULL," +
	"device_count INT DEFAULT NULL," +
	"potential_rights_end TIMESTAMPTZ DEFAULT NULL," +
	"license_ref VARCHAR(255) NOT NULL," +
	"rights_end TIMESTAMPTZ DEFAULT NULL  " +
	");" +
	"CREATE INDEX IF NOT EXISTS license_ref_index on license_status (license_ref);"
