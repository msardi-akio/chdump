package main

import (
	"database/sql"
	"fmt"
	"flag"
	"log"
	"slices"
	"strings"

	_ "github.com/ClickHouse/clickhouse-go/v2"
)

func main() {

	excludeDefaultDBs := flag.Bool("exclude-default", true, "Exclude default and system DBs")
	clusterName := flag.String("cluster-name", "", "Name of the target cluster")
	clickhouseDSN := flag.String("conn-string", "", "The ClickHouse connection string")
	flag.Parse()
	

	systemDBs := []string{
		"INFORMATION_SCHEMA",
		"default",
		"information_schema",
		"system",
	}

	if *clickhouseDSN == "" {
		log.Fatalf("Missing required argument conn-string")
	}
	if *clusterName == "" {
		log.Fatalf("Missing required argument cluster-name")
	}

	// Establish a connection to the database
	db, err := sql.Open("clickhouse", *clickhouseDSN)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	// Ping the database to ensure connection is established
	if err := db.Ping(); err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer db.Close()

	// If all databases are wanted, query for them
	var databases []string
	databaseNamesQuery := "SHOW DATABASES"
	rows, err := db.Query(databaseNamesQuery)
	if err != nil {
		log.Fatalf("Failed to execute query: %v", err)
	}
	defer rows.Close()
	var databaseName string
	for rows.Next() {
		if err := rows.Scan(&databaseName); err != nil {
			log.Fatalf("Failed to scan row: %v", err)
		}
		// Exclude default and system databases
		if (*excludeDefaultDBs) {
			// Append only non-default databases
			if !(slices.Contains(systemDBs, databaseName)) {
				databases = append(databases, databaseName)
			}
		} else {
			// Append all
			databases = append(databases, databaseName)
		}
	}


	for _, databaseName := range databases {
		newDSN := fmt.Sprintf("%s/%s", *clickhouseDSN, databaseName)
		db, err := sql.Open("clickhouse", newDSN)
		if err != nil {
			log.Fatalf("Failed to open database: %v", err)
		}
		// Ping the database to ensure connection is established
		if err := db.Ping(); err != nil {
			log.Fatalf("Failed to connect: %v", err)
		}
		defer db.Close()

		// Query to retrieve the DDL of each database
		showCreateDatabaseQuery := fmt.Sprintf("SHOW CREATE DATABASE `%s`", databaseName)
		databaseDDL, err := db.Query(showCreateDatabaseQuery)
		if err != nil {
			log.Fatalf("Failed to get DDL for database %s: %v", databaseName, err)
		}

		if databaseDDL.Next() {
			var ddl string
			if err := databaseDDL.Scan(&ddl); err != nil {
				log.Fatalf("Failed to scan DDL of database %s: %v", databaseName, err)
			}
			// New version for printing database definitions - append the ON CLUSTER clause to create tables on all replicas
			//fmt.Printf("%s\n;\n----------------------------------------\n", ddl)
			ddl = strings.Replace(ddl, "CREATE DATABASE", "CREATE DATABASE IF NOT EXISTS", 1)
			for i, line := range strings.Split(strings.TrimSuffix(ddl, "\n"), "\n") {
				fmt.Println(line)
				if (i == 0) {
					fmt.Printf("ON CLUSTER %s\n", *clusterName)
				}
			}
			fmt.Printf("\n;\n----------------------------------------\n")
		}
		// Set the database to use in the resulting sql file
		fmt.Printf("USE %s\n;\n----------------------------------------\n", databaseName)
		defer databaseDDL.Close()

		// Query to retrieve table names in the specified database
		tableNamesQuery := "SHOW TABLES"
		rows, err := db.Query(tableNamesQuery)
		if err != nil {
			log.Fatalf("Failed to execute query: %v", err)
		}
		defer rows.Close()

		var tableName string
		for rows.Next() {
			if err := rows.Scan(&tableName); err != nil {
				log.Fatalf("Failed to scan row: %v", err)
			}
			// Query to retrieve the DDL of each table
			showCreateTableQuery := fmt.Sprintf("SHOW CREATE TABLE `%s`", tableName)
			tableDDL, err := db.Query(showCreateTableQuery)
			if err != nil {
				log.Fatalf("Failed to get DDL for table %s: %v", tableName, err)
			}
			defer tableDDL.Close()

			if tableDDL.Next() {
				var ddl string
				if err := tableDDL.Scan(&ddl); err != nil {
					log.Fatalf("Failed to scan DDL of table %s: %v", tableName, err)
				}
				// New version for printing table definitions - append the ON CLUSTER clause to create tables on all replicas
				//fmt.Printf("%s\n;\n----------------------------------------\n", ddl)

				// Add the IF NOT EXISTS on each CREATE for idempotence
				ddl = strings.Replace(ddl, "CREATE TABLE", "CREATE TABLE IF NOT EXISTS", 1)
				ddl = strings.Replace(ddl, "CREATE VIEW", "CREATE VIEW IF NOT EXISTS", 1)
				ddl = strings.Replace(ddl, "CREATE MATERIALIZED VIEW", "CREATE MATERIALIZED VIEW IF NOT EXISTS", 1)

				for i, line := range strings.Split(strings.TrimSuffix(ddl, "\n"), "\n") {
					fmt.Println(line)
					if (i == 0) {
						fmt.Printf("ON CLUSTER %s\n", *clusterName)
					}
				}
				fmt.Printf("\n;\n----------------------------------------\n")
			}
		}

		if err := rows.Err(); err != nil {
			log.Fatalf("Error during row iteration: %v", err)
		}
	}
} 
