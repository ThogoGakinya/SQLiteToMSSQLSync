using System;
using System.Data.SQLite;
using System.Data.SqlClient;
using System.Diagnostics.Metrics;

namespace SQLiteToMSSQLSync
{
    public class Program
    {
        // SQLite and MSSQL connection parameters
        static string sqliteDbPath = @"C:\Users\invoice\AppData\Local\Your Apps Ltd\SmartIntegrationPDF\config\SmartIntegration.db";
        static string mssqlConnectionString = @"server=KF-MYQSRV01; database=KFCu; uid=sa; pwd=#Kenya@3020";
        int Counter = 0;

        static void Main(string[] args)
        {
            try
            {
                while (true)
                {
                    // Start monitoring SQLite for changes
                    MonitorSQLite();

                    System.Threading.Thread.Sleep(5000);
                }
            }
            catch (Exception e) 
            {
                Console.WriteLine(e.Message);
                Console.ReadLine();
            }
                
        }

        static void MonitorSQLite()
        {
            using (SQLiteConnection sqliteConn = new SQLiteConnection($"Data Source={sqliteDbPath};Version=3;"))
            using (SqlConnection mssqlConn = new SqlConnection(mssqlConnectionString))
            {
                try
                {
                    sqliteConn.Open();
                    mssqlConn.Open();

                    SQLiteCommand sqliteCmd = sqliteConn.CreateCommand();
                    sqliteCmd.CommandText = "SELECT * FROM TransactionHeader WHERE sync_status = 0 ";
                    SQLiteDataReader reader = sqliteCmd.ExecuteReader();

                    while (reader.Read())
                    {
                        // Insert the data into MSSQL
                        InsertIntoMSSQL(mssqlConn, reader);
                        // Mark record as synced in SQLite
                        MarkAsSynced(sqliteConn, Convert.ToInt32(reader["Id"]));
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message);
                    Console.ReadLine();
                }
            }

            static void InsertIntoMSSQL(SqlConnection mssqlConn, SQLiteDataReader reader)
            {
                SqlCommand mssqlCmd = mssqlConn.CreateCommand();
                mssqlCmd.CommandText = "INSERT INTO TransactionHeader (TrnType, CompanyName, ClientPINnum, HeadQuarters, RelatedInvoiceNum, TraderSystemInvNum, CUNo, QRCodeUrl, TrnDate, SubTotA, VatTotal, CuSerialNo) VALUES" +
                    " (@TrnType, @CompanyName, @ClientPINnum, @HeadQuarters, @RelatedInvoiceNum, @TraderSystemInvNum, @CUNo, @QRCodeUrl, @TrnDate, @SubTotA, @VatTotal, @CuSerialNo)";
                mssqlCmd.Parameters.AddWithValue("@TrnType", reader["TrnType"]);
                mssqlCmd.Parameters.AddWithValue("@CompanyName", reader["CompanyName"]);
                mssqlCmd.Parameters.AddWithValue("@ClientPINnum", reader["ClientPINnum"]);
                mssqlCmd.Parameters.AddWithValue("@HeadQuarters", reader["HeadQuarters"]);
                mssqlCmd.Parameters.AddWithValue("@RelatedInvoiceNum", reader["RelatedInvoiceNum"]);
                mssqlCmd.Parameters.AddWithValue("@TraderSystemInvNum", reader["TraderSystemInvNum"]);
                mssqlCmd.Parameters.AddWithValue("@CUNo", reader["CUNo"]);
                mssqlCmd.Parameters.AddWithValue("@QRCodeUrl", reader["QRCodeUrl"]);
                mssqlCmd.Parameters.AddWithValue("@TrnDate", reader["TrnDate"]);
                mssqlCmd.Parameters.AddWithValue("@SubTotA", reader["SubTotA"]);
                mssqlCmd.Parameters.AddWithValue("@VatTotal", reader["VatTotal"]);
                mssqlCmd.Parameters.AddWithValue("@CuSerialNo", reader["CuSerialNo"]);
              
                mssqlCmd.ExecuteNonQuery();
            }

            static void MarkAsSynced(SQLiteConnection sqliteConn, int recordId)
            {
                SQLiteCommand sqliteCmd = sqliteConn.CreateCommand();
                sqliteCmd.CommandText = "UPDATE TransactionHeader SET d = '1' WHERE id = @id";
                sqliteCmd.Parameters.AddWithValue("@id", recordId);

                sqliteCmd.ExecuteNonQuery();
            }
        }
    }
}
