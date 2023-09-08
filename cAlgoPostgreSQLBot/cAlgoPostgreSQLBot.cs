using System;
using cAlgo.API;
using cAlgo.API.Internals;
using Npgsql;

namespace cAlgo
{
    [Robot(TimeZone = TimeZones.UTC, AccessRights = AccessRights.FullAccess)]
    public class PostgreSQL : Robot
    {
        // PostgreSQL connection settings
        private readonly string dbName = "fxhistory";
        private readonly string dbHost = "localhost";
        private readonly int dbPort = 5432;
        private readonly string dbUser = "test";
        private readonly string dbPass = "test";
        private readonly string dbSchema = "public";

        //
        // Globals
        //
        private NpgsqlDataSource dataSource;
        private NpgsqlCommand cmd;
        private int diagTicks; // Diagnostic counter for ticks

        // Prints exception details into cBot Log
        private void MyErrorHandler(Object sender, Exception ex, string myRoutine)
        {
            string senderString;
            if (sender != null)
                senderString = sender.ToString();
            else
                senderString = "N/A";
            Print("(EE) Routine: " + myRoutine + " Sender:  " + senderString + " Details:" + ex.ToString());
        }

        // Bot start routine. Put your initialization logic here.
        protected override void OnStart()
        {
            Print("Starting with symbol code: " + Symbol.Name);
            // Build database connection string
            NpgsqlConnectionStringBuilder sb = new()
            {
                Host = dbHost,
                Port = dbPort,
                Database = dbName,
                Username = dbUser,
                Password = dbPass,
                Timeout = 5,
                CommandTimeout = 5
            };
            Print("Database connection string: " + sb.ConnectionString);
            dataSource = NpgsqlDataSource.Create(sb.ConnectionString);

            try
            {
                // Example: SELECT EXISTS ( SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = 'audusd')
                string commandText = "SELECT EXISTS ( SELECT 1 FROM pg_tables WHERE schemaname = '" + dbSchema + "' AND tablename = '" + Symbol.Name.ToLower() + "')";
                Print("SQL> " + commandText);
                cmd = dataSource.CreateCommand(commandText);
                bool tableExisting = (bool)cmd.ExecuteScalar();
                Print("SQL query result: " + tableExisting.ToString());
                if (!tableExisting)
                {
                    // Example: CREATE TABLE public.audusd ("id" bigserial, "utc" timestamp without time zone, "tick" bigint, "ask" real, "bid" real, "spread" real)
                    commandText = "CREATE TABLE " + dbSchema + "." + Symbol.Name.ToLower() + " (\"id\" bigserial, \"utc\" timestamp without time zone, \"tick\" bigint, \"ask\" real, \"bid\" real, \"spread\" real)";
                    Print("SQL> " + commandText);
                    cmd = dataSource.CreateCommand(commandText);
                    int execResult = cmd.ExecuteNonQuery();
                    Print("SQL query result: " + execResult.ToString() + " rows affected.");
                }
            } 
            catch (Exception ex)
            {
                MyErrorHandler(this, ex, "OnStart()");
                Print("Database problems => Robot will be stopped...");
                Stop();
            }
        }

        // Executes every tick. Put your core logic here.
        protected override void OnTick()
        {
            // Ticks are 100-ns intervals elapsed since January 1, 0001 at 00:00:00.000 in the Gregorian calendar.
            DateTime serverTime = Server.Time;
            // Example: INSERT INTO public.audusd(utc, tick, ask, bid, spread) VALUES('2023-01-01 00:00:01.000', 2, 3, 4, 5)
            string commandText = "INSERT INTO " + dbSchema + "." + Symbol.Name + " (utc, tick, ask, bid, spread) VALUES (";
            commandText += "'" + serverTime.ToString("yyyy-MM-dd HH:mm:ss.fff") + "', ";
            commandText += serverTime.Ticks.ToString() + ", ";
            commandText += Symbol.Ask.ToString() + ", ";
            commandText += Symbol.Bid.ToString() + ", ";
            commandText += Symbol.Spread.ToString() + ")";
            Print("SQL> " + commandText);
            cmd = dataSource.CreateCommand(commandText);
            int execResult = cmd.ExecuteNonQuery();
            Print("SQL query result: " + execResult.ToString() + " rows affected.");

            //
            // -- ON SCREEN DIAGNOSTICS --
            //
            // Index of the last/newest bar on chart
            int barLast = Bars.OpenTimes.GetIndexByTime(Bars.OpenTimes.LastValue);
            Chart.DrawText("barLast", (barLast).ToString(), barLast, Bars.HighPrices.LastValue + 0.0001, Color.Azure);
            // Diagnostic text
            ++diagTicks;
            string msg = Symbol.Name + " " + " Ask: " + Symbol.Ask.ToString("0.00000") + " Bid: " + Symbol.Bid.ToString("0.00000");
            msg += " Spread: " + Symbol.Spread.ToString("0.00000") + Environment.NewLine;
            msg += "SQL command: " + commandText + Environment.NewLine;
            msg += "SQL query result: " + execResult.ToString() + " rows affected." + Environment.NewLine;
            msg += "Ticks: " + diagTicks.ToString() + Environment.NewLine;
            // Draw text on the diagram
            Chart.DrawStaticText("diaginfo", msg, VerticalAlignment.Top, HorizontalAlignment.Left, Color.White);
        }

        // On Stop routine. Put your de-initialization logic here.
        protected override void OnStop()
        {
        }
    }
}
