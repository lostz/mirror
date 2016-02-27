package main

import (
	"fmt"
	"github.com/lostz/mirror/mysql"
)

func main() {
	parser := mysql.NewBinlogParser()
	err := parser.ParseFile("mysql-bin.000008", 0)
	if err != nil {
		fmt.Println(err.Error())
	}

}
