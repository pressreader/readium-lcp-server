// Copyright 2020 Readium Foundation. All rights reserved.
// Use of this source code is governed by a BSD-style license
// that can be found in the LICENSE file exposed on Github (readium) in the project repository.

package transactions

import (
	"database/sql"
	"errors"
	"log"
	"strings"
	"time"

	"github.com/readium/readium-lcp-server/config"
	"github.com/readium/readium-lcp-server/status"
)

var NotFound = errors.New("Event not found")

type Transactions interface {
	Get(id int) (Event, error)
	Add(e Event, eventType int) error
	GetByLicenseStatusId(licenseStatusFk int) func() (Event, error)
	CheckDeviceStatus(licenseStatusFk int, deviceId string) (string, error)
	ListRegisteredDevices(licenseStatusFk int) func() (Device, error)
}

type RegisteredDevicesList struct {
	ID      string   `json:"id"`
	Devices []Device `json:"devices"`
}

type Device struct {
	DeviceId   string    `json:"id"`
	DeviceName string    `json:"name"`
	Timestamp  time.Time `json:"timestamp"`
}

type Event struct {
	ID              int       `json:"-"`
	DeviceName      string    `json:"name"`
	Timestamp       time.Time `json:"timestamp"`
	Type            string    `json:"type"`
	DeviceId        string    `json:"id"`
	LicenseStatusFk int       `json:"-"`
}

type dbTransactions struct {
	db                    *sql.DB
	get                   *sql.Stmt
	add                   *sql.Stmt
	getbylicensestatusid  *sql.Stmt
	checkdevicestatus     *sql.Stmt
	listregistereddevices *sql.Stmt
}

// Get returns an event by its id
//
func (i dbTransactions) Get(id int) (Event, error) {
	records, err := i.get.Query(id)
	if err != nil {
		return Event{}, err
	}

	var typeInt int

	defer records.Close()
	if records.Next() {
		var e Event
		err = records.Scan(&e.ID, &e.DeviceName, &e.Timestamp, &typeInt, &e.DeviceId, &e.LicenseStatusFk)
		if err == nil {
			e.Type = status.EventTypes[typeInt]
		}
		return e, err
	}

	return Event{}, NotFound
}

// Add adds an event in the database,
// The parameter eventType corresponds to the field 'type' in table 'event'
//
func (i dbTransactions) Add(e Event, eventType int) error {
	_, err := i.add.Exec(e.DeviceName, e.Timestamp, eventType, e.DeviceId, e.LicenseStatusFk)
	return err
}

// GetByLicenseStatusId returns all events by license status id
//
func (i dbTransactions) GetByLicenseStatusId(licenseStatusFk int) func() (Event, error) {
	rows, err := i.getbylicensestatusid.Query(licenseStatusFk)
	if err != nil {
		return func() (Event, error) { return Event{}, err }
	}
	return func() (Event, error) {
		var e Event
		var err error
		var typeInt int

		if rows.Next() {
			err = rows.Scan(&e.ID, &e.DeviceName, &e.Timestamp, &typeInt, &e.DeviceId, &e.LicenseStatusFk)
			if err == nil {
				e.Type = status.EventTypes[typeInt]
			}
		} else {
			rows.Close()
			err = NotFound
		}
		return e, err
	}
}

// ListRegisteredDevices returns all devices which have an 'active' status by licensestatus id
//
func (i dbTransactions) ListRegisteredDevices(licenseStatusFk int) func() (Device, error) {
	rows, err := i.listregistereddevices.Query(licenseStatusFk)
	if err != nil {
		return func() (Device, error) { return Device{}, err }
	}
	return func() (Device, error) {
		var d Device
		var err error
		if rows.Next() {
			err = rows.Scan(&d.DeviceId, &d.DeviceName, &d.Timestamp)
		} else {
			rows.Close()
			err = NotFound
		}
		return d, err
	}
}

// CheckDeviceStatus gets the current status of a device
// if the device has not been recorded in the 'event' table, typeString is empty.
//
func (i dbTransactions) CheckDeviceStatus(licenseStatusFk int, deviceId string) (string, error) {
	var typeString string
	var typeInt int

	row := i.checkdevicestatus.QueryRow(licenseStatusFk, deviceId)
	err := row.Scan(&typeInt)

	if err == nil {
		typeString = status.EventTypes[typeInt]
	} else {
		if err == sql.ErrNoRows {
			return typeString, nil
		}
	}

	return typeString, err
}

// Open defines scripts for queries & create the 'event' table if it does not exist
//
func Open(db *sql.DB) (t Transactions, err error) {

	var createTableQuery, getQuery, getByLicenseStatusIdQuery, checkDeviceStatusQuery, addQuery, listRegisteredDevicesQuery string
	if strings.HasPrefix(config.Config.LcpServer.Database, "postgres") {
		// postgres
		createTableQuery = tableDefPostgres
		getQuery = "SELECT * FROM event WHERE id = $1 LIMIT 1"
		getByLicenseStatusIdQuery = "SELECT * FROM event WHERE license_status_fk = $1"
		checkDeviceStatusQuery = "SELECT type FROM event WHERE license_status_fk = $1 AND device_id = $2 ORDER BY timestamp DESC LIMIT 1"
		listRegisteredDevicesQuery = "SELECT device_id, device_name, timestamp FROM event WHERE license_status_fk = $1 AND type = 1"
		addQuery = "INSERT INTO event (device_name, timestamp, type, device_id, license_status_fk) VALUES ($1, $2, $3, $4, $5)"
	} else {
		// mysql/sqlite
		createTableQuery = tableDef
		getQuery = "SELECT * FROM event WHERE id = ? LIMIT 1"
		getByLicenseStatusIdQuery = "SELECT * FROM event WHERE license_status_fk = ?"
		checkDeviceStatusQuery = "SELECT type FROM event WHERE license_status_fk = ? AND device_id = ? ORDER BY timestamp DESC LIMIT 1"
		listRegisteredDevicesQuery = "SELECT device_id, device_name, timestamp FROM event WHERE license_status_fk = ? AND type = 1"
		addQuery = "INSERT INTO event (device_name, timestamp, type, device_id, license_status_fk) VALUES (?, ?, ?, ?, ?)"
	}

	// if sqlite/postgres, create the event table in the lsd db if it does not exist
	if strings.HasPrefix(config.Config.LsdServer.Database, "sqlite") || strings.HasPrefix(config.Config.LcpServer.Database, "postgres") {
		_, err = db.Exec(createTableQuery)
		if err != nil {
			log.Println("Error creating sqlite event table")
			return
		}
	}

	// select an event by its id
	get, err := db.Prepare(getQuery)
	if err != nil {
		return
	}

	// add an event
	add, err := db.Prepare(addQuery)
	if err != nil {
		return
	}

	getbylicensestatusid, err := db.Prepare(getByLicenseStatusIdQuery)
	if err != nil {
		return
	}

	// the status of a device corresponds to the latest event stored in the db.
	checkdevicestatus, err := db.Prepare(checkDeviceStatusQuery)
	if err != nil {
		return
	}

	listregistereddevices, err := db.Prepare(listRegisteredDevicesQuery)
	if err != nil {
		return
	}

	t = dbTransactions{db, get, add, getbylicensestatusid, checkdevicestatus, listregistereddevices}
	return
}

const tableDef = "CREATE TABLE IF NOT EXISTS event (" +
	"id integer PRIMARY KEY," +
	"device_name varchar(255) DEFAULT NULL," +
	"timestamp datetime NOT NULL," +
	"type int NOT NULL," +
	"device_id varchar(255) DEFAULT NULL," +
	"license_status_fk int NOT NULL," +
	"FOREIGN KEY(license_status_fk) REFERENCES license_status(id)" +
	");" +
	"CREATE INDEX IF NOT EXISTS license_status_fk_index on event (license_status_fk);"

const tableDefPostgres = "CREATE TABLE IF NOT EXISTS event (" +
	"id SERIAL PRIMARY KEY," +
	"device_name VARCHAR(255) DEFAULT NULL," +
	"timestamp TIMESTAMPTZ NOT NULL," +
	"type INT NOT NULL," +
	"device_id VARCHAR(255) DEFAULT NULL," +
	"license_status_fk INT NOT NULL," +
	"FOREIGN KEY(license_status_fk) REFERENCES license_status(id)" +
	");" +
	"CREATE INDEX IF NOT EXISTS license_status_fk_index on event (license_status_fk);"
