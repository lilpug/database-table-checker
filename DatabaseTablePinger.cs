using DotNetSDB;
using System;
using System.Diagnostics;

namespace Database.Testing
{
	/// <summary>
	/// A quick diagnostic class to log if a databases tables are being slow
	/// </summary>
	public static class DatabaseTablePinger
	{
		/// <summary>
		/// This function creates a log table that will then be used by the RunPinger function
		/// </summary>
		/// <param name="loggerDB"></param>
		public static void CreateLogTable(SQLServer2016 loggerDB)
		{	
		    if (!loggerDB.table_exist("log"))
		    {
			loggerDB.add_create_table("log", new string[] { "id", "value", "generated" },
							 new string[] { "int Identity(1,1) PRIMARY KEY not null", "varchar(max)", "datetime not null" });
			loggerDB.run();
		    }            
		}

		/// <summary>
		/// This function runs a basic query against all tables within a database and logs any slow calls
		/// </summary>
		/// <param name="loggerDB">The database object for the logger db and table location</param>
		/// <param name="checkerDB">The database object for the database to be checked</param>
		/// <param name="databaseName">The database name that you want to check</param>
		public static void RunPinger(SQLServer2016 loggerDB, SQLServer2016 checkerDB, string databaseName)
		{   
			checkerDB.add_select("INFORMATION_SCHEMA.TABLES", "TABLE_NAME");
			checkerDB.add_where_normal("INFORMATION_SCHEMA.TABLES", "TABLE_TYPE", "BASE TABLE");
			checkerDB.add_where_normal("INFORMATION_SCHEMA.TABLES", "TABLE_CATALOG", databaseName);
			string[] tables = checkerDB.run_return_string_array();

			foreach(string table in tables)
			{
				// Create new stopwatch.
				Stopwatch stopwatch = new Stopwatch();

				try
				{
					// Begin timing.
					stopwatch.Start();

					checkerDB.add_pure_sql($"select top 1 1 from {table}");                            
					string temp = checkerDB.run_return_string();

					// Stop timing.
					stopwatch.Stop();

					if(stopwatch.Elapsed.Seconds > 5)
					{
						throw new Exception($"Table {table} took {stopwatch.Elapsed.Seconds} seconds to finish.");
					}
				}
				catch (Exception e)
				{
					string error = $"Table: {table}{Environment.NewLine}Error: {e.Message}";

					loggerDB.add_insert("log", new object[] { error, DateTime.Now });
					loggerDB.run();                                						
				}
				finally
				{
					stopwatch.Stop();
					stopwatch = null;
				}
			}			
		}
	}
}
