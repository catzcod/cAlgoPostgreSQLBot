using System;
using System.Linq;
using cAlgo.API;
using cAlgo.API.Indicators;
using cAlgo.API.Internals;
using cAlgo.Indicators;
using Npgsql;

namespace cAlgo
{
    [Robot(TimeZone = TimeZones.UTC, AccessRights = AccessRights.FullAccess)]
    public class PostgreSQL : Robot
    {
        // PostgreSQL connection settings
        string dbName = "fxhistory";
        string dbHost = "localhost";
        int dbPort = 5432;
        string dbUser = "testuser";
        string dbPass = "test";
        string dbSchema = "public";
        //
        // Globals
        //
        NpgsqlConnection dbConn;
        // Diagnostic counter for ticks 1-2-3-4-1-2-etc
        int diagTicks;

        /// <summary>
        /// Prints exception details into cBot Log
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="ex"></param>
        /// <param name="myRoutine"></param>
        private void MyErrorHandler(Object sender, Exception ex, string myRoutine)
        {
            string senderString;
            if (sender != null)
                senderString = sender.ToString();
            else
                senderString = "N/A";
            Print("(EE) Routine: " + myRoutine + " Sender:  " + senderString + " Details:" + ex.ToString());
        }

        /// <summary>
        /// Bot start routine. Put your initialization logic here.
        /// </summary>
        protected override void OnStart()
        {
            Print("Starting with symbol code: " + Symbol.Code);
            // Try to connect to DB
            NpgsqlConnectionStringBuilder sb = new NpgsqlConnectionStringBuilder();
            sb.Host = dbHost;
            sb.Port = dbPort;
            sb.Database = dbName;
            sb.Username = dbUser;
            sb.Password = dbPass;
            sb.Timeout = 5;
            sb.CommandTimeout = 5;
            Print("Database connection string: " + sb.ConnectionString);
            dbConn = new NpgsqlConnection(sb.ConnectionString);
            try
            {
                dbConn.Open();
            } catch (Exception ex)
            {
                MyErrorHandler(this, ex, "OnStart()");
                Print("Database cannot be opened => Robot will be stopped...");
                Stop();
            }
            Print("DB connection: " + dbConn.FullState.ToString());
            // Information_schema: Whether a table (or view) exists, and the current user has access to it?
            // SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE  table_schema = 'schema_name' AND table_name = 'table_name');
            // System catalog: How to check whether a table exists?
            // SELECT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'schema_name' AND tablename = 'table_name');
            // Create table if not existing
            // CREATE TABLE public.table_name ("column_name" column_type, etc...)
            try
            {
                using (NpgsqlCommand cmd = new NpgsqlCommand())
                {
                    cmd.Connection = dbConn;
                    cmd.CommandText = "SELECT EXISTS ( SELECT 1 FROM pg_tables WHERE schemaname = '" + dbSchema + "' AND tablename = '" + Symbol.Code.ToLower() + "')";
                    Print("SQL> " + cmd.CommandText);
                    bool tableExisting = (bool)cmd.ExecuteScalar();
                    Print("SQL query result: " + tableExisting.ToString());
                    if (!tableExisting)
                    {
                        cmd.CommandText = "CREATE TABLE " + dbSchema + "." + Symbol.Code.ToLower() + " (\"utc\" timestamp without time zone, \"tick\" bigint, \"ask\" real, \"bid\" real, \"spread\" real)";
                        Print("SQL> " + cmd.CommandText);
                        cmd.ExecuteNonQuery();
                    }
                }
            } catch (Exception ex)
            {
                MyErrorHandler(this, ex, "OnStart()");
                Print("Database problems => Robot will be stopped...");
                dbConn.Close();
                Stop();
            }
        }

        /// <summary>
        /// Executes every tick. Put your core logic here.
        /// </summary>
        protected override void OnTick()
        {
            // Ticks are 100-ns intervals elapsed since January 1, 0001 at 00:00:00.000 in the Gregorian calendar.
            DateTime serverTime = Server.Time;
            using (NpgsqlCommand cmd = new NpgsqlCommand())
            {
                cmd.Connection = dbConn;
                cmd.CommandText = "INSERT INTO " + dbSchema + "." + Symbol.Code + " (utc, tick, ask, bid, spread) VALUES (:utc, :tick, :ask, :bid, :spread)";
                cmd.Parameters.AddWithValue(":utc", NpgsqlTypes.NpgsqlDbType.Timestamp, serverTime.ToString("yyyy-MM-dd HH:mm:ss.fff"));
                cmd.Parameters.AddWithValue(":tick", serverTime.Ticks);
                cmd.Parameters.AddWithValue(":ask", Symbol.Ask);
                cmd.Parameters.AddWithValue(":bid", Symbol.Bid);
                cmd.Parameters.AddWithValue(":spread", Symbol.Spread);
                cmd.ExecuteNonQuery();
            }
            //
            // -- ON SCREEN DIAGNOSTICS --
            //
            // Index of the last/newest bar on chart
            int barLast = MarketSeries.OpenTime.GetIndexByTime(MarketSeries.OpenTime.LastValue);
            ChartObjects.DrawText("barLast", (barLast).ToString(), barLast, MarketSeries.High.LastValue + 0.0001);
            // Diagnostic text
            ++diagTicks;
            if (diagTicks >= 4)
                diagTicks = 0;
            char ctick;
            switch (diagTicks)
            {
                case 0:
                    ctick = '-';
                    break;
                case 1:
                    ctick = '\\';
                    break;
                case 2:
                    ctick = '|';
                    break;
                case 3:
                    ctick = '/';
                    break;
                default:
                    ctick = '.';
                    break;
            }
            string msg = diagTicks.ToString() + " " + ctick + Environment.NewLine;
            msg = msg + Symbol.Code + ">" + " Ask: " + Symbol.Ask.ToString("0.00000") + " Bid: " + Symbol.Bid.ToString("0.00000") + " Spread: " + Symbol.Spread.ToString("0.00000") + Environment.NewLine;
            ChartObjects.DrawText("diaginfo", msg, StaticPosition.TopLeft);
        }

        /// <summary>
        /// On Stop routine. Put your de-initialization logic here.
        /// </summary>
        protected override void OnStop()
        {
            dbConn.Close();
            Print("DB connection: " + dbConn.FullState.ToString());
        }
    }
}
