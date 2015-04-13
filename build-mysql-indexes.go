package main

import (
	"bufio"
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"os"
	"strings"
)

func main() {

	// Ask for the column name
	username := flag.String("u", "root", "Username")
	password := flag.String("p", "pass", "Password")
	address := flag.String("h", "localhost", "Hostname")
	port := flag.Int("P", 3306, "Port")
	database := flag.String("D", "mysql", "Database")
	column := flag.String("column", "updated", "Column to be indexed")

	flag.Parse()

	db, err := sql.Open("mysql", fmt.Sprintf("%v:%v@tcp(%v:%v)/%v", *username, *password, *address, *port, *database))
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()
	err = db.Ping()
	if err != nil {
		fmt.Println("Failed to connect to the database: " + err.Error())
		os.Exit(1)
	}

	// Run the SQL statement to find all tables and whether they already have indexes
	var (
		name string
		hasColumn bool
		hasIndex bool
	)
	rows, err := db.Query(fmt.Sprintf(`select t.table_name,
		max(case when column_name = 'updated' then 1 else 0 end) as has_column,
		case when x.table_name is not null then true else false end as has_index
		from information_schema.columns t left join 
			(select distinct table_name 
			from information_schema.statistics 
			where table_schema = '%v' and column_name = '%v' and seq_in_index = 1) x on t.table_name = x.table_name
		where table_schema = '%v' group by t.table_name;`, *database, *column, *database))
	defer rows.Close()
	var haveIndex, needIndex, needColumn []string
	for rows.Next() {
		err := rows.Scan(&name, &hasColumn, &hasIndex)
		if err != nil {
			panic(err.Error())
		}
		if !hasColumn && !hasIndex {
			needColumn = append(needColumn, name)
		} else if !hasIndex {
			needIndex = append(needIndex, name)
		} else {
			haveIndex = append(haveIndex, name)
		}
	}
	err = rows.Err()
	if err != nil {
		panic(err.Error())
	}

	// Print the tables that will have indexes added already, the ones that will have indexes added,
	// and the ones that don't have the required column. Ask for confirmation.
	if len(haveIndex) > 0 {
		fmt.Printf("\nThe following tables have the '%v' column indexed:\n%v\n", *column, strings.Join(haveIndex, ", "))
	}
	if len(needColumn) > 0 {
		fmt.Printf("\nThe following tables don't have the '%v' column:\n%v\n", *column, strings.Join(needColumn, ", "))
	} else {
		fmt.Printf("\nAll tables have the '%v' column.\n", *column)
	}
	if len(needIndex) > 0 {
		fmt.Printf("\nAn index on the '%v' column named 'index_<table>_on_%v' will be added to the following tables:\n%v\n", *column, *column, strings.Join(needIndex, ", "))
	} else {
		fmt.Printf("\nNo indexes to add\n")
	}

	// Add each index in turn, print OK in between.
	if len(needIndex) == 0 || !confirm() {
		os.Exit(0)
	}
	for _, table := range needIndex {
		fmt.Println("Adding index to " + table)
		rows, err := db.Query(fmt.Sprintf("alter table `%v` add key `index_%v_on_%v` (`%v`)", table, table, *column, *column))
		defer rows.Close()
		if err != nil {
			panic(err.Error())
		}
	}
}

func confirm() bool {
	reader := bufio.NewReader(os.Stdin)
	fmt.Print("Build these indexes now? [Y/n]: ")
	text, _ := reader.ReadString('\n')
	response := strings.TrimSpace(text)
	if response == "y" || response == "Y" || response == "" {
		return true
	} else if response == "n" || response == "N" {
		return false
	} else {
		return confirm()
	}
}
