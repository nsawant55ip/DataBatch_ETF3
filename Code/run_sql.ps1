###############################################################
# Name: Run_sql.ps1
# Date: Sep 7 2015
# Author: Dinesh/Devesh
#
# Note: Powershell script to connect to sql server
#       only to save output as csv with quotes.
#       All other methods seemed primitive and of course
#       this is MS SQL (Windows), so no good libraries
#       in python
###############################################################
#Accepting Named Parameters
Param(
  [string]$Queryloc,
  [string]$CSVPath
)

#Connection Strings
$Database = "qai"
$Server = "ainmg1-vwsql01"


#Replace the name of the .sql file to read here
$SqlQuery = [System.IO.File]::ReadAllText($Queryloc)

# To Get the Query from the Files
try {
    $SqlConnection = New-Object System.Data.SqlClient.SqlConnection
    $SqlConnection.ConnectionString = "Data Source=$Server;Initial Catalog=$Database;Integrated Security = True"
    $SqlConnection.Open()
}
catch {
    echo "Error occured in DataBase connection : $_.Exception.Message"
    exit 1
}

$SqlCmd = New-Object System.Data.SqlClient.SqlCommand 
$SqlCmd.CommandText = $SqlQuery 
$SqlCmd.Connection = $SqlConnection 
$Sqlcmd.CommandTimeout = 36000

# Connect to SQL and query data, extract data to SQL Adapter 
try {
    $SqlAdapter = New-Object System.Data.SqlClient.SqlDataAdapter
    $SqlAdapter.SelectCommand = $SqlCmd
    $DataSet = New-Object System.Data.DataSet 
    $nRecs = $SqlAdapter.Fill($DataSet) 
    $SqlConnection.Close()
    $nRecs | Out-Null

    #Populate Hash Table
    $objTable = $DataSet.Tables[0]

    #Export Hash Table to CSV File
    $objTable | Export-CSV $CSVPath -NotypeInformation
}
catch {
    echo "Error occured in SQL Command:$_.Exception.Message"
    exit 2
}
