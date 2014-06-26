// --------------------------------------------------------------------------------------------------------------------
// <summary>
//   UIMF Data Writer class
//
//   Written by Yan Shi for the Department of Energy (PNNL, Richland, WA)
//   Additional contributions by Anuj Shah, Matthew Monroe, Gordon Slysz, Kevin Crowell, and Bill Danielson
//   E-mail: matthew.monroe@pnnl.gov or proteomics@pnl.gov
//   Website: http://omics.pnl.gov/software/
//
// </summary>
// --------------------------------------------------------------------------------------------------------------------

using System.Linq;

namespace UIMFLibrary
{
	using System;
	using System.Collections.Generic;
	using System.Data.SQLite;
	using System.Text;

	/// <summary>
	/// UIMF Data Writer class
	/// </summary>
	public class DataWriter : IDisposable
	{
		#region Fields

		/// <summary>
		/// Track whether frame parameter columns have been verified
		/// </summary>
		private bool m_FrameParameterColumnsVerified;

		/// <summary>
		/// Command to insert a frame
		/// </summary>
		private SQLiteCommand m_dbCommandPrepareInsertFrame;

		/// <summary>
		/// Command to insert a scan
		/// </summary>
		private SQLiteCommand m_dbCommandPrepareInsertScan;

		/// <summary>
		/// Command to insert scan parameters.
		/// </summary>
		private SQLiteCommand m_dbCommandPrepareInsertScanParameters;

		/// <summary>
		/// General database update command
		/// </summary>
		private SQLiteCommand m_dbCommandUimf;

		/// <summary>
		/// Connection to the database
		/// </summary>
		private SQLiteConnection m_dbConnection;

		/// <summary>
		/// Full path to the UIMF file
		/// </summary>
		private readonly string m_fileName;

		/// <summary>
		/// Global parameters object
		/// </summary>
		private GlobalParameters m_globalParameters;

		/// <summary>
		/// Tracks whether the scan parameters table was created
		/// </summary>
		private readonly bool m_isScanParameterTable;

		#endregion

		#region Constructors and Destructors

		/// <summary>
		/// Initializes a new instance of the <see cref="DataWriter"/> class. 
		/// Constructor for UIMF datawriter that takes the filename and begins the transaction. 
		/// </summary>
		/// <param name="fileName">
		/// Full path to the data file
		/// </param>
		/// <param name="createScanParameters">
		/// True if the scan parameters table should be created
		/// </param>
		public DataWriter(string fileName, bool createScanParameters = false)
		{
			this.m_fileName = fileName;
			this.m_isScanParameterTable = createScanParameters;
			string connectionString = "Data Source = " + fileName + "; Version=3; DateTimeFormat=Ticks;";
			this.m_dbConnection = new SQLiteConnection(connectionString);
			try
			{
				this.m_dbConnection.Open();

				// Note that the following call will instantiate m_dbCommandUimf
				this.TransactionBegin();

				this.PrepareInsertFrame();
				this.PrepareInsertScan();

				this.m_FrameParameterColumnsVerified = false;
			}
			catch (Exception ex)
			{
				Console.WriteLine("Exception opening the UIMF file: " + ex.Message);
			}
		}

		#endregion

		#region Public Methods and Operators

		/// <summary>
		/// Post a new log entry to table Log_Entries
		/// </summary>
		/// <param name="oConnection">
		/// Database connection object
		/// </param>
		/// <param name="EntryType">
		/// Log entry type (typically Normal, Error, or Warning)
		/// </param>
		/// <param name="Message">
		/// Log message
		/// </param>
		/// <param name="PostedBy">
		/// Process or application posting the log message
		/// </param>
		/// <remarks>
		/// The Log_Entries table will be created if it doesn't exist
		/// </remarks>
		public static void PostLogEntry(SQLiteConnection oConnection, string EntryType, string Message, string PostedBy)
		{
			// Check whether the Log_Entries table needs to be created
			SQLiteCommand cmdPostLogEntry = oConnection.CreateCommand();

			if (!DataReader.TableExists(oConnection, "Log_Entries"))
			{
				// Log_Entries not found; need to create it
				cmdPostLogEntry.CommandText = "CREATE TABLE Log_Entries ( " +
												"Entry_ID INTEGER PRIMARY KEY, " +
												"Posted_By STRING, " +
												"Posting_Time STRING, " +
												"Type STRING, " +
												"Message STRING)";

				cmdPostLogEntry.ExecuteNonQuery();
			}

			if (string.IsNullOrEmpty(EntryType))
			{
				EntryType = "Normal";
			}

			if (string.IsNullOrEmpty(PostedBy))
			{
				PostedBy = string.Empty;
			}

			if (string.IsNullOrEmpty(Message))
			{
				Message = string.Empty;
			}

			// Now add a log entry
			cmdPostLogEntry.CommandText = "INSERT INTO Log_Entries (Posting_Time, Posted_By, Type, Message) " + "VALUES ("
										  + "datetime('now'), " + "'" + PostedBy + "', " + "'" + EntryType + "', " + "'"
										  + Message + "')";

			cmdPostLogEntry.ExecuteNonQuery();
			cmdPostLogEntry.Dispose();
		}

		/// <summary>
		/// Add a column to the Frame_Parameters table
		/// </summary>
		/// <param name="parameterName">
		/// </param>
		/// <param name="parameterType">
		/// </param>
		/// <remarks>
		/// The new column will have Null values for all existing rows
		/// </remarks>
		public void AddFrameParameter(string parameterName, string parameterType)
		{
			try
			{
				this.m_dbCommandUimf = this.m_dbConnection.CreateCommand();
				this.m_dbCommandUimf.CommandText = "Alter TABLE Frame_Parameters Add " + parameterName + " " + parameterType;
				this.m_dbCommandUimf.ExecuteNonQuery();
				this.m_dbCommandUimf.Dispose();
			}
			catch (Exception ex)
			{
				Console.WriteLine("Error adding parameter " + parameterName + " to the Frame_Parameters table:" + ex.Message);
			}
		}

		/// <summary>
		/// Add a column to the Frame_Parameters table
		/// </summary>
		/// <param name="parameterName">
		/// Parameter name (aka column name in the database)
		/// </param>
		/// <param name="parameterType">
		/// Parameter type
		/// </param>
		/// <param name="defaultValue">
		/// Value to assign to all rows
		/// </param>
		public void AddFrameParameter(string parameterName, string parameterType, int defaultValue)
		{
			this.AddFrameParameter(parameterName, parameterType);

			try
			{
				this.m_dbCommandUimf = this.m_dbConnection.CreateCommand();
				this.m_dbCommandUimf.CommandText = "UPDATE Frame_Parameters SET " + parameterName + " = " + defaultValue
												   + " WHERE " + parameterName + " IS NULL";
				this.m_dbCommandUimf.ExecuteNonQuery();

				this.m_dbCommandUimf.Dispose();
			}
			catch (Exception ex)
			{
				Console.WriteLine("Error setting default value for parameter " + parameterName + ": " + ex.Message);
			}
		}

		/// <summary>
		/// Add a column to the Frame_Parameters table
		/// </summary>
		/// <param name="parameterName">
		/// Parameter name (aka column name in the database)
		/// </param>
		/// <param name="parameterType">
		/// Parameter type
		/// </param>
		/// <param name="defaultValue">
		/// Value to assign to all rows
		/// </param>
		public void AddFrameParameter(string parameterName, string parameterType, string defaultValue)
		{
			this.AddFrameParameter(parameterName, parameterType);

			try
			{
				this.m_dbCommandUimf = this.m_dbConnection.CreateCommand();
				this.m_dbCommandUimf.CommandText = "UPDATE Frame_Parameters SET " + parameterName + " = '" + defaultValue
												   + "' WHERE " + parameterName + " IS NULL";
				this.m_dbCommandUimf.ExecuteNonQuery();

				this.m_dbCommandUimf.Dispose();
			}
			catch (Exception ex)
			{
				Console.WriteLine("Error setting default value for parameter " + parameterName + ": " + ex.Message);
			}
		}

		/// <summary>
		/// </summary>
		/// <param name="parameterName">
		/// </param>
		/// <param name="parameterType">
		/// </param>
		/// <param name="parameterValue">
		/// </param>
		public void AddGlobalParameter(string parameterName, string parameterType, string parameterValue)
		{
			try
			{
				this.m_dbCommandUimf = this.m_dbConnection.CreateCommand();
				this.m_dbCommandUimf.CommandText = "Alter TABLE Global_Parameters Add " + parameterName + " "
												   + parameterType;
				this.m_dbCommandUimf.CommandText += " UPDATE Global_Parameters SET " + parameterName + " = "
													+ parameterValue;
				this.m_dbCommandUimf.ExecuteNonQuery();
				this.m_dbCommandUimf.Dispose();
			}
			catch
			{
				this.m_dbCommandUimf = this.m_dbConnection.CreateCommand();
				this.m_dbCommandUimf.CommandText = "UPDATE Global_Parameters SET " + parameterName + " = "
												   + parameterValue;
				this.m_dbCommandUimf.ExecuteNonQuery();
				this.m_dbCommandUimf.Dispose();
				Console.WriteLine("Parameter " + parameterName + " already exists, its value will be updated to " + parameterValue);
			}
		}

		/// <summary>
		/// This function will create tables that are bin centric (as opposed to scan centric) to allow querying of the data in 2 different ways. 
		/// Bin centric data is important for data access speed in informed workflows.
		/// </summary>
		public void CreateBinCentricTables()
		{
			this.CreateBinCentricTables(string.Empty);
		}

		/// <summary>
		/// This function will create tables that are bin centric (as opposed to scan centric) to allow querying of the data in 2 different ways. 
		/// Bin centric data is important for data access speed in informed workflows.
		/// </summary>
		/// <param name="workingDirectory">
		/// Path to the working directory in which a temporary SqLite database file should be created
		/// </param>
		public void CreateBinCentricTables(string workingDirectory)
		{
			using (var uimfReader = new DataReader(this.m_fileName))
			{
				var binCentricTableCreator = new BinCentricTableCreation();
				binCentricTableCreator.CreateBinCentricTable(this.m_dbConnection, uimfReader, workingDirectory);
			}
		}

		/// <summary>
		/// Create the table struture within a UIMF file, assumes 32-bit integers for intensity values 
		/// </summary>
		/// <remarks>
		/// This must be called after opening a new file to create the default tables that are required for IMS data.
		/// </remarks>
		public void CreateTables()
		{
			const string dataType = "int";

			CreateTables(dataType);
		}

		/// <summary>
		/// Create the table struture within a UIMF file
		/// </summary>
		/// <param name="dataType">
		/// Data type of intensity in the Frame_Scans table: Double, float short, or int
		/// </param>
		/// <remarks>
		/// This must be called after opening a new file to create the default tables that are required for IMS data.
		/// </remarks>
		public void CreateTables(string dataType)
		{
			// https://prismwiki.pnl.gov/wiki/IMS_Data_Processing

			this.m_dbCommandUimf = this.m_dbConnection.CreateCommand();

			// Create the Global_Parameters Table  
			var lstFields = GetGlobalParametersFields();
			this.m_dbCommandUimf.CommandText = GetCreateTableSql("Global_Parameters", lstFields);
			this.m_dbCommandUimf.ExecuteNonQuery();

			// Create the Frame_parameters Table
			lstFields = GetFrameParametersFields();
			this.m_dbCommandUimf.CommandText = GetCreateTableSql("Frame_Parameters", lstFields);
			this.m_dbCommandUimf.ExecuteNonQuery();

			// Re-initialize m_dbCommandPrepareInsertFrame so that it uses voltEntranceHPFIn and voltEntranceHPFOut
			this.PrepareInsertFrame();

			// Create the Frame_Scans Table
			lstFields = GetFrameScansFields(dataType);
			this.m_dbCommandUimf.CommandText = GetCreateTableSql("Frame_Scans", lstFields);

			// Facilitate faster retrieval of scans/spectrums.
			this.m_dbCommandUimf.CommandText += "CREATE UNIQUE INDEX pk_index on Frame_Scans(FrameNum, ScanNum);";
			this.m_dbCommandUimf.ExecuteNonQuery();

			if (this.m_isScanParameterTable)
			{
				this.m_dbCommandUimf.CommandText = "CREATE TABLE Scan_Parameters ( " +
													 "ScanNum INTEGER NOT NULL, " +
													 "MS_Level SMALLINT NOT NULL);";
				this.m_dbCommandUimf.CommandText += "CREATE UNIQUE INDEX scan_index on Scan_Parameters(ScanNum, MS_Level);";
				this.m_dbCommandUimf.ExecuteNonQuery();
			}

			this.m_dbCommandUimf.Dispose();
		}

		/// <summary>
		/// Deletes the scans for all frames in the file.  In addition, updates the Scans column to 0 in Frame_Parameters for all frames.
		/// </summary>
		/// <param name="frameType">
		/// </param>
		/// <param name="updateScanCountInFrameParams">
		/// If true, then will update the Scans column to be 0 for the deleted frames
		/// </param>
		/// <param name="bShrinkDatabaseAfterDelete">
		/// </param>
		/// <remarks>
		/// As an alternative to using this function, use CloneUIMF() in the DataReader class
		/// </remarks>
		public void DeleteAllFrameScans(int frameType, bool updateScanCountInFrameParams, bool bShrinkDatabaseAfterDelete)
		{
			this.m_dbCommandUimf = this.m_dbConnection.CreateCommand();

			this.m_dbCommandUimf.CommandText = "DELETE FROM Frame_Scans " + "WHERE FrameNum IN (SELECT FrameNum " +
											   "FROM Frame_Parameters " + "WHERE FrameType = " + frameType + ");";
			this.m_dbCommandUimf.ExecuteNonQuery();

			if (updateScanCountInFrameParams)
			{
				this.m_dbCommandUimf.CommandText = "UPDATE Frame_Parameters " + "SET Scans = 0 " + "WHERE FrameType = "
												   + frameType + ";";
				this.m_dbCommandUimf.ExecuteNonQuery();
			}

			// Commmit the currently open transaction
			this.TransactionCommit();
			System.Threading.Thread.Sleep(100);

			if (bShrinkDatabaseAfterDelete)
			{
				this.m_dbCommandUimf.CommandText = "VACUUM;";
				this.m_dbCommandUimf.ExecuteNonQuery();
			}

			// Open a new transaction
			this.TransactionBegin();

			this.m_dbCommandUimf.Dispose();
		}

		/// <summary>
		/// Deletes the frame from the Frame_Parameters table and from the Frame_Scans table
		/// </summary>
		/// <param name="frameNum">
		/// </param>
		/// <param name="updateGlobalParameters">
		/// If true, then decrements the NumFrames value in the Global_Parameters table
		/// </param>
		public void DeleteFrame(int frameNum, bool updateGlobalParameters)
		{
			this.m_dbCommandUimf = this.m_dbConnection.CreateCommand();

			this.m_dbCommandUimf.CommandText = "DELETE FROM Frame_Scans WHERE FrameNum = " + frameNum + "; ";
			this.m_dbCommandUimf.ExecuteNonQuery();

			this.m_dbCommandUimf.CommandText = "DELETE FROM Frame_Parameters WHERE FrameNum = " + frameNum + "; ";
			this.m_dbCommandUimf.ExecuteNonQuery();

			if (updateGlobalParameters)
			{
				this.m_dbCommandUimf.CommandText = "UPDATE Global_Parameters SET NumFrames = NumFrames - 1 WHERE NumFrames > 0; ";
				this.m_dbCommandUimf.ExecuteNonQuery();
			}

			this.m_dbCommandUimf.Dispose();

			this.FlushUimf();
		}

		/// <summary>
		/// Deletes all of the scans for the specified frame
		/// </summary>
		/// <param name="frameNum">
		/// The frame number to delete
		/// </param>
		/// <param name="updateScanCountInFrameParams">
		/// If true, then will update the Scans column to be 0 for the deleted frames
		/// </param>
		public void DeleteFrameScans(int frameNum, bool updateScanCountInFrameParams)
		{
			this.m_dbCommandUimf = this.m_dbConnection.CreateCommand();

			this.m_dbCommandUimf.CommandText = "DELETE FROM Frame_Scans WHERE FrameNum = " + frameNum + "; ";
			this.m_dbCommandUimf.ExecuteNonQuery();

			if (updateScanCountInFrameParams)
			{
				this.m_dbCommandUimf.CommandText = "UPDATE Frame_Parameters SET Scans = 0 WHERE FrameNum = " + frameNum
												   + "; ";
				this.m_dbCommandUimf.ExecuteNonQuery();
			}

			this.m_dbCommandUimf.Dispose();

			this.FlushUimf();
		}

		/// <summary>
		/// Deletes given frames from the UIMF file. 
		/// </summary>
		/// <param name="frameNums">
		/// </param>
		/// <param name="updateGlobalParameters">
		/// </param>
		public void DeleteFrames(List<int> frameNums, bool updateGlobalParameters)
		{
			var sFrameList = new StringBuilder();

			// Construct a comma-separated list of frame numbers
			foreach (int frameNum in frameNums)
			{
				sFrameList.Append(frameNum + ",");
			}

			this.m_dbCommandUimf = this.m_dbConnection.CreateCommand();

			this.m_dbCommandUimf.CommandText = "DELETE FROM Frame_Scans WHERE FrameNum IN (" + sFrameList.ToString().TrimEnd(',')
											   + "); ";
			this.m_dbCommandUimf.ExecuteNonQuery();

			this.m_dbCommandUimf.CommandText = "DELETE FROM Frame_Parameters WHERE FrameNum IN ("
											   + sFrameList.ToString().TrimEnd(',') + "); ";
			this.m_dbCommandUimf.ExecuteNonQuery();

			if (updateGlobalParameters)
			{
				this.m_dbCommandUimf.CommandText = "UPDATE Global_Parameters SET NumFrames = NumFrames - " + frameNums.Count + "; ";
				this.m_dbCommandUimf.ExecuteNonQuery();

				// Make sure NumFrames is >= 0
				this.m_dbCommandUimf.CommandText = "SELECT NumFrames FROM Global_Parameters; ";
				object objResult = this.m_dbCommandUimf.ExecuteScalar();

				if (Convert.ToInt32(objResult) < 0)
				{
					this.m_dbCommandUimf.CommandText = "UPDATE Global_Parameters SET NumFrames 0; ";
					this.m_dbCommandUimf.ExecuteNonQuery();
				}
			}

			this.m_dbCommandUimf.Dispose();

			this.FlushUimf();
		}

		/// <summary>
		/// Dispose of any system resources
		/// </summary>
		public void Dispose()
		{
			try
			{
				Dispose(true);
				GC.SuppressFinalize(this);
			}
			// ReSharper disable once EmptyGeneralCatchClause
			catch
			{
				// Ignore errors here
			}
		}

		/// <summary>
		/// Dispose of any system resources
		/// </summary>
		/// <param name="disposing">
		/// True when disposing
		/// </param>
		protected virtual void Dispose(bool disposing)
		{
			if (disposing)
			{
				if (this.m_dbConnection != null)
				{
					this.TransactionCommit();

					this.m_dbCommandUimf.Dispose();
					this.m_dbConnection.Close();
					this.m_dbConnection = null;
				}
			}
		}

		/// <summary>
		/// Commits the currently open transaction, then starts a new one
		/// </summary>
		/// <remarks>
		/// Note that a transaction is started when the UIMF file is opened, then commited when the class is disposed
		/// </remarks>
		public void FlushUimf()
		{
			this.TransactionCommit();
			System.Threading.Thread.Sleep(100);
			this.TransactionBegin();
		}

		/// <summary>
		/// Get global parameters
		/// </summary>
		/// <returns>
		/// Global parameters class<see cref="GlobalParameters"/>.
		/// </returns>
		public GlobalParameters GetGlobalParameters()
		{
			return DataReader.GetGlobalParametersFromTable(this.m_dbConnection);
		}

		/// <summary>
		/// Method to insert details related to each IMS frame
		/// </summary>
		/// <param name="frameParameters">
		/// </param>
		public void InsertFrame(FrameParameters frameParameters)
		{
			if (this.m_dbCommandPrepareInsertFrame == null)
			{
				this.PrepareInsertFrame();
			}

			// Make sure the Frame_Parameters table has all of the required columns
			this.ValidateFrameParameterColumns();

			this.m_dbCommandPrepareInsertFrame.Parameters.Clear();

			// Frame number (primary key)     
			this.m_dbCommandPrepareInsertFrame.Parameters.Add(new SQLiteParameter(":FrameNum", frameParameters.FrameNum));

			// Start time of frame, in minutes
			this.m_dbCommandPrepareInsertFrame.Parameters.Add(new SQLiteParameter(":StartTime", frameParameters.StartTime));

			// Duration of frame, in seconds 
			this.m_dbCommandPrepareInsertFrame.Parameters.Add(new SQLiteParameter(":Duration", frameParameters.Duration));

			// Number of collected and summed acquisitions in a frame 
			this.m_dbCommandPrepareInsertFrame.Parameters.Add(
				new SQLiteParameter(":Accumulations", frameParameters.Accumulations));

			// Bitmap: 0=MS (Legacy); 1=MS (Regular); 2=MS/MS (Frag); 3=Calibration; 4=Prescan
			this.m_dbCommandPrepareInsertFrame.Parameters.Add(new SQLiteParameter(":FrameType", (int)frameParameters.FrameType));

			// Number of TOF scans  
			this.m_dbCommandPrepareInsertFrame.Parameters.Add(new SQLiteParameter(":Scans", frameParameters.Scans));

			// IMFProfile Name; this stores the name of the sequence used to encode the data when acquiring data multiplexed
			this.m_dbCommandPrepareInsertFrame.Parameters.Add(new SQLiteParameter(":IMFProfile", frameParameters.IMFProfile));

			// Number of TOF Losses
			this.m_dbCommandPrepareInsertFrame.Parameters.Add(new SQLiteParameter(":TOFLosses", frameParameters.TOFLosses));

			// Average time between TOF trigger pulses
			this.m_dbCommandPrepareInsertFrame.Parameters.Add(
				new SQLiteParameter(":AverageTOFLength", frameParameters.AverageTOFLength));

			// Value of k0  
			this.m_dbCommandPrepareInsertFrame.Parameters.Add(
				new SQLiteParameter(":CalibrationSlope", frameParameters.CalibrationSlope));

			// Value of t0  
			this.m_dbCommandPrepareInsertFrame.Parameters.Add(
				new SQLiteParameter(":CalibrationIntercept", frameParameters.CalibrationIntercept));

			// These six parameters below are coefficients for residual mass error correction      
			//   ResidualMassError=a2t+b2t^3+c2t^5+d2t^7+e2t^9+f2t^11
			this.m_dbCommandPrepareInsertFrame.Parameters.Add(new SQLiteParameter(":a2", frameParameters.a2));
			this.m_dbCommandPrepareInsertFrame.Parameters.Add(new SQLiteParameter(":b2", frameParameters.b2));
			this.m_dbCommandPrepareInsertFrame.Parameters.Add(new SQLiteParameter(":c2", frameParameters.c2)); // 13
			this.m_dbCommandPrepareInsertFrame.Parameters.Add(new SQLiteParameter(":d2", frameParameters.d2)); // 14
			this.m_dbCommandPrepareInsertFrame.Parameters.Add(new SQLiteParameter(":e2", frameParameters.e2)); // 15
			this.m_dbCommandPrepareInsertFrame.Parameters.Add(new SQLiteParameter(":f2", frameParameters.f2)); // 16

			// Ambient temperature
			this.m_dbCommandPrepareInsertFrame.Parameters.Add(new SQLiteParameter(":Temperature", frameParameters.Temperature));

			// Voltage setting in the IMS system
			this.m_dbCommandPrepareInsertFrame.Parameters.Add(new SQLiteParameter(":voltHVRack1", frameParameters.voltHVRack1));

			// Voltage setting in the IMS system
			this.m_dbCommandPrepareInsertFrame.Parameters.Add(new SQLiteParameter(":voltHVRack2", frameParameters.voltHVRack2));

			// Voltage setting in the IMS system
			this.m_dbCommandPrepareInsertFrame.Parameters.Add(new SQLiteParameter(":voltHVRack3", frameParameters.voltHVRack3));

			// Voltage setting in the IMS system
			this.m_dbCommandPrepareInsertFrame.Parameters.Add(new SQLiteParameter(":voltHVRack4", frameParameters.voltHVRack4));

			// Capillary Inlet Voltage
			this.m_dbCommandPrepareInsertFrame.Parameters.Add(new SQLiteParameter(":voltCapInlet", frameParameters.voltCapInlet));


			if (DataReader.ColumnExists(this.m_dbConnection, "Frame_Parameters", "voltEntranceHPFIn"))
			{
				// HPF In Voltage
				this.m_dbCommandPrepareInsertFrame.Parameters.Add(
					new SQLiteParameter(":voltEntranceHPFIn", frameParameters.voltEntranceHPFIn));

				// HPF Out Voltage
				this.m_dbCommandPrepareInsertFrame.Parameters.Add(
					new SQLiteParameter(":voltEntranceHPFOut", frameParameters.voltEntranceHPFOut));
			}
			else
			{
				// IFT In Voltage
				this.m_dbCommandPrepareInsertFrame.Parameters.Add(
					new SQLiteParameter(":voltEntranceIFTIn", frameParameters.voltEntranceHPFIn));

				// IFT Out Voltage
				this.m_dbCommandPrepareInsertFrame.Parameters.Add(
					new SQLiteParameter(":voltEntranceIFTOut", frameParameters.voltEntranceHPFOut));
			}

			// Cond Limit Voltage
			this.m_dbCommandPrepareInsertFrame.Parameters.Add(
				new SQLiteParameter(":voltEntranceCondLmt", frameParameters.voltEntranceCondLmt));

			// Trap Out Voltage
			this.m_dbCommandPrepareInsertFrame.Parameters.Add(new SQLiteParameter(":voltTrapOut", frameParameters.voltTrapOut));

			// Trap In Voltage
			this.m_dbCommandPrepareInsertFrame.Parameters.Add(new SQLiteParameter(":voltTrapIn", frameParameters.voltTrapIn));

			// Jet Disruptor Voltage
			this.m_dbCommandPrepareInsertFrame.Parameters.Add(new SQLiteParameter(":voltJetDist", frameParameters.voltJetDist));

			// Fragmentation Quadrupole Voltage
			this.m_dbCommandPrepareInsertFrame.Parameters.Add(new SQLiteParameter(":voltQuad1", frameParameters.voltQuad1));

			// Fragmentation Conductance Voltage
			this.m_dbCommandPrepareInsertFrame.Parameters.Add(new SQLiteParameter(":voltCond1", frameParameters.voltCond1));

			// Fragmentation Quadrupole Voltage
			this.m_dbCommandPrepareInsertFrame.Parameters.Add(new SQLiteParameter(":voltQuad2", frameParameters.voltQuad2));

			// Fragmentation Conductance Voltage
			this.m_dbCommandPrepareInsertFrame.Parameters.Add(new SQLiteParameter(":voltCond2", frameParameters.voltCond2));

			// IMS Out Voltage
			this.m_dbCommandPrepareInsertFrame.Parameters.Add(new SQLiteParameter(":voltIMSOut", frameParameters.voltIMSOut));

			if (DataReader.ColumnExists(this.m_dbConnection, "Frame_Parameters", "voltExitHPFIn"))
			{
				// HPF In Voltage
				this.m_dbCommandPrepareInsertFrame.Parameters.Add(
					new SQLiteParameter(":voltExitHPFIn", frameParameters.voltExitHPFIn));

				// HPF Out Voltage
				this.m_dbCommandPrepareInsertFrame.Parameters.Add(
					new SQLiteParameter(":voltExitHPFOut", frameParameters.voltExitHPFOut));
			}
			else
			{
				// IFT In Voltage
				this.m_dbCommandPrepareInsertFrame.Parameters.Add(
					new SQLiteParameter(":voltExitIFTIn", frameParameters.voltExitHPFIn));

				// IFT Out Voltage
				this.m_dbCommandPrepareInsertFrame.Parameters.Add(
					new SQLiteParameter(":voltExitIFTOut", frameParameters.voltExitHPFOut));
			}

			// Cond Limit Voltage
			this.m_dbCommandPrepareInsertFrame.Parameters.Add(
				new SQLiteParameter(":voltExitCondLmt", frameParameters.voltExitCondLmt));

			// Pressure at front of Drift Tube 
			this.m_dbCommandPrepareInsertFrame.Parameters.Add(
				new SQLiteParameter(":PressureFront", frameParameters.PressureFront));

			// Pressure at back of Drift Tube 
			this.m_dbCommandPrepareInsertFrame.Parameters.Add(new SQLiteParameter(":PressureBack", frameParameters.PressureBack));

			// Determines original size of bit sequence
			this.m_dbCommandPrepareInsertFrame.Parameters.Add(new SQLiteParameter(":MPBitOrder", frameParameters.MPBitOrder));

			// Voltage profile used in fragmentation
			this.m_dbCommandPrepareInsertFrame.Parameters.Add(
				new SQLiteParameter(":FragmentationProfile", ConvertToBlob(frameParameters.FragmentationProfile)));


			this.m_dbCommandPrepareInsertFrame.Parameters.Add(
				new SQLiteParameter(":HPPressure", frameParameters.HighPressureFunnelPressure));
			this.m_dbCommandPrepareInsertFrame.Parameters.Add(
				new SQLiteParameter(":IPTrapPressure", frameParameters.IonFunnelTrapPressure));
			this.m_dbCommandPrepareInsertFrame.Parameters.Add(
				new SQLiteParameter(":RIFunnelPressure", frameParameters.RearIonFunnelPressure));
			this.m_dbCommandPrepareInsertFrame.Parameters.Add(
				new SQLiteParameter(":QuadPressure", frameParameters.QuadrupolePressure));

			this.m_dbCommandPrepareInsertFrame.Parameters.Add(new SQLiteParameter(":ESIVoltage", frameParameters.ESIVoltage));

			this.m_dbCommandPrepareInsertFrame.Parameters.Add(new SQLiteParameter(":FloatVoltage", frameParameters.FloatVoltage));

			// Set to 1 after a frame has been calibrated
			this.m_dbCommandPrepareInsertFrame.Parameters.Add(
				new SQLiteParameter(":CalibrationDone", frameParameters.CalibrationDone));

			// Set to 1 after a frame has been decoded (added June 27, 2011)
			this.m_dbCommandPrepareInsertFrame.Parameters.Add(new SQLiteParameter(":Decoded", frameParameters.Decoded));

			this.m_dbCommandPrepareInsertFrame.ExecuteNonQuery();
			this.m_dbCommandPrepareInsertFrame.Parameters.Clear();
		}

		/// <summary>
		/// Method to enter the details of the global parameters for the experiment
		/// </summary>
		/// <param name="header">
		/// </param>
		public void InsertGlobal(GlobalParameters header)
		{
			this.m_globalParameters = header;
			this.m_dbCommandUimf = this.m_dbConnection.CreateCommand();
			this.m_dbCommandUimf.CommandText = "INSERT INTO Global_Parameters "
											   + "(DateStarted, NumFrames, TimeOffset, BinWidth, Bins, TOFCorrectionTime, FrameDataBlobVersion, ScanDataBlobVersion, "
											   + "TOFIntensityType, DatasetType, Prescan_TOFPulses, Prescan_Accumulations, Prescan_TICThreshold, Prescan_Continuous, Prescan_Profile, Instrument_name) "
											   + "VALUES(:DateStarted, :NumFrames, :TimeOffset, :BinWidth, :Bins, :TOFCorrectionTime, :FrameDataBlobVersion, :ScanDataBlobVersion, "
											   + ":TOFIntensityType, :DatasetType, :Prescan_TOFPulses, :Prescan_Accumulations, :Prescan_TICThreshold, :Prescan_Continuous, :Prescan_Profile, :Instrument_name);";

			this.m_dbCommandUimf.Parameters.Add(new SQLiteParameter(":DateStarted", header.DateStarted));
			this.m_dbCommandUimf.Parameters.Add(new SQLiteParameter(":NumFrames", header.NumFrames));
			this.m_dbCommandUimf.Parameters.Add(new SQLiteParameter(":TimeOffset", header.TimeOffset));
			this.m_dbCommandUimf.Parameters.Add(new SQLiteParameter(":BinWidth", header.BinWidth));
			this.m_dbCommandUimf.Parameters.Add(new SQLiteParameter(":Bins", header.Bins));
			this.m_dbCommandUimf.Parameters.Add(new SQLiteParameter(":TOFCorrectionTime", header.TOFCorrectionTime));
			this.m_dbCommandUimf.Parameters.Add(new SQLiteParameter(":FrameDataBlobVersion", header.FrameDataBlobVersion));
			this.m_dbCommandUimf.Parameters.Add(new SQLiteParameter(":ScanDataBlobVersion", header.ScanDataBlobVersion));
			this.m_dbCommandUimf.Parameters.Add(new SQLiteParameter(":TOFIntensityType", header.TOFIntensityType));
			this.m_dbCommandUimf.Parameters.Add(new SQLiteParameter(":DatasetType", header.DatasetType));
			this.m_dbCommandUimf.Parameters.Add(new SQLiteParameter(":Prescan_TOFPulses", header.Prescan_TOFPulses));
			this.m_dbCommandUimf.Parameters.Add(new SQLiteParameter(":Prescan_Accumulations", header.Prescan_Accumulations));
			this.m_dbCommandUimf.Parameters.Add(new SQLiteParameter(":Prescan_TICThreshold", header.Prescan_TICThreshold));
			this.m_dbCommandUimf.Parameters.Add(new SQLiteParameter(":Prescan_Continuous", header.Prescan_Continuous));
			this.m_dbCommandUimf.Parameters.Add(new SQLiteParameter(":Prescan_Profile", header.Prescan_Profile));
			this.m_dbCommandUimf.Parameters.Add(new SQLiteParameter(":Instrument_name", header.InstrumentName));

			this.m_dbCommandUimf.ExecuteNonQuery();
			this.m_dbCommandUimf.Parameters.Clear();
			this.m_dbCommandUimf.Dispose();
		}

		/// <summary>
		/// Write out the compressed intensity data to the UIMF file
		/// </summary>
		/// <param name="frameParameters"></param>
		/// <param name="scanNum"></param>
		/// <param name="binWidth"></param>
		/// <param name="indexOfMaxIntensity"></param>
		/// <param name="nonZeroCount"></param>
		/// <param name="bpi"></param>
		/// <param name="tic"></param>
		/// <param name="spectra"></param>
		private void InsertScanStoreBytes(
			FrameParameters frameParameters, 
			int scanNum, 
			double binWidth, 
			int indexOfMaxIntensity,
			int nonZeroCount, 
			double bpi, 
			double tic, 
			byte[] spectra)
		{
			if (nonZeroCount <= 0)
				return;

			var bpiMz = this.ConvertBinToMz(indexOfMaxIntensity, binWidth, frameParameters);

			// Insert records
			this.InsertScanAddParameters(frameParameters.FrameNum, scanNum, nonZeroCount, (int)bpi, bpiMz, (int)tic, spectra);
			this.m_dbCommandPrepareInsertScan.ExecuteNonQuery();
			this.m_dbCommandPrepareInsertScan.Parameters.Clear();
		}

		/// <summary>
		/// Insert a new scan using an array of intensities (as ints) along with binWidth
		/// </summary>
		/// <param name="frameParameters">
		/// Frame parameters
		/// </param>
		/// <param name="scanNum">
		/// Scan number
		/// </param>
		/// <param name="intensities">
		/// Array of intensities, including all zeros
		/// </param>
		/// <param name="binWidth">
		/// Bin width (used to compute m/z value of the BPI data point)
		/// </param>
		/// <returns>
		/// Number of non-zero data points
		/// </returns>
		/// <remarks>
		/// The intensities array should contain an intensity for every bin, including all of the zeroes
		/// </remarks>
		public int InsertScan(
			FrameParameters frameParameters,
			int scanNum,
			int[] intensities,
			double binWidth)
		{
			byte[] spectra;
			double tic;
			double bpi;
			int indexOfMaxIntensity;

			int nonZeroCount = IntensityConverterInt32.Encode(frameParameters, intensities, out spectra, out tic, out bpi, out indexOfMaxIntensity);

			InsertScanStoreBytes(frameParameters, scanNum, binWidth, indexOfMaxIntensity, nonZeroCount, bpi, tic, spectra);

			return nonZeroCount;
		}

		/// <summary>
		/// Insert a new scan using an array of intensities (as shorts) and binWidth
		/// </summary>
		/// <param name="frameParameters">
		/// </param>
		/// <param name="scanNum">
		/// </param>
		/// <param name="intensities">
		/// </param>
		/// <param name="binWidth">
		/// </param>
		/// <returns>
		/// The size of the compressed archive in the output buffer<see cref="int"/>.
		/// </returns>
		/// <remarks>
		/// The intensities array should contain an intensity for every bin, including all of the zeroes
		/// </remarks>
		public int InsertScan(
			FrameParameters frameParameters,
			int scanNum,
			short[] intensities,
			double binWidth)
		{
			byte[] spectra;
			double tic;
			double bpi;
			int indexOfMaxIntensity;

			int nonZeroCount = IntensityConverterInt16.Encode(frameParameters, intensities, out spectra, out tic, out bpi, out indexOfMaxIntensity);

			InsertScanStoreBytes(frameParameters, scanNum, binWidth, indexOfMaxIntensity, nonZeroCount, bpi, tic, spectra);

			return nonZeroCount;
		}

		/// <summary>
		/// Insert a new scan using an array of intensities (as floats) and binWidth
		/// </summary>
		/// <param name="frameParameters">
		/// Frame parameters.
		/// </param>
		/// <param name="scanNum">
		/// Scan num.
		/// </param>
		/// <param name="intensities">
		/// Intensities array
		/// </param>
		/// <param name="binWidth">
		/// Bin width
		/// </param>
		/// <returns>
		/// The size of the compressed archive in the output buffer<see cref="int"/>.
		/// </returns>
		/// <remarks>
		/// The intensities array should contain an intensity for every bin, including all of the zeroes
		/// </remarks>
		public int InsertScan(
			FrameParameters frameParameters,
			int scanNum,
			float[] intensities,
			double binWidth)
		{
			byte[] spectra;
			double tic;
			double bpi;
			int indexOfMaxIntensity;

			int nonZeroCount = IntensityConverterFloat.Encode(frameParameters, intensities, out spectra, out tic, out bpi, out indexOfMaxIntensity);

			InsertScanStoreBytes(frameParameters, scanNum, binWidth, indexOfMaxIntensity, nonZeroCount, bpi, tic, spectra);

			return nonZeroCount;
		}

		/// <summary>
		/// Insert a new scan using an array of intensities (as doubles) and binWidth
		/// </summary>
		/// <param name="frameParameters">
		/// Frame parameters.
		/// </param>
		/// <param name="scanNum">
		/// Scan num.
		/// </param>
		/// <param name="intensities">
		/// Intensities array
		/// </param>
		/// <param name="binWidth">
		/// Bin width
		/// </param>
		/// <returns>
		/// The size of the compressed archive in the output buffer<see cref="int"/>.
		/// </returns>
		/// <remarks>
		/// The intensities array should contain an intensity for every bin, including all of the zeroes
		/// </remarks>
		public int InsertScan(
			FrameParameters frameParameters,
			int scanNum,
			double[] intensities,
			double binWidth)
		{
			byte[] spectra;
			double tic;
			double bpi;
			int indexOfMaxIntensity;

			int nonZeroCount = IntensityConverterDouble.Encode(frameParameters, intensities, out spectra, out tic, out bpi, out indexOfMaxIntensity);

			InsertScanStoreBytes(frameParameters, scanNum, binWidth, indexOfMaxIntensity, nonZeroCount, bpi, tic, spectra);

			return nonZeroCount;
		}

		/// <summary>
		/// This method takes in a list of intensity information by bin and converts the data to a run length encoded array
		/// which is later compressed at the byte level for reduced size
		/// </summary>
		/// <param name="frameParameters">
		/// </param>
		/// <param name="scanNum">
		/// </param>
		/// <param name="binToIntensityMap">
		/// Keys are bin numbers and values are intensity values; intensity values are assumed to all be non-zero
		/// </param>
		/// <param name="binWidth">
		/// </param>
		/// <param name="timeOffset">
		/// </param>
		/// <returns>
		/// Non-zero data count<see cref="int"/>.
		/// </returns>
		public int InsertScan(
			FrameParameters frameParameters,
			int scanNum,
			List<KeyValuePair<int, int>> binToIntensityMap,
			double binWidth,
			int timeOffset)
		{
			if (frameParameters == null)
				return -1;

			if (binToIntensityMap == null || binToIntensityMap.Count == 0)
			{
				return 0;
			}

			int arraySize = binToIntensityMap.Count;

			// RLZE - convert 0s to negative multiples as well as calculate TIC and BPI, BPI_MZ
			var rlzeDataList = new List<int>();

			int tic = 0;
			int bpi = 0;
			const byte dataTypeSize = 4;
			int indexOfMaxIntensity = 0;

			if (this.m_globalParameters == null)
			{
				this.m_globalParameters = DataReader.GetGlobalParametersFromTable(this.m_dbConnection);
			}

			// Calculate TIC and BPI while run length zero encoding
			int previousBin = int.MinValue;

			rlzeDataList.Add(-(timeOffset + binToIntensityMap[0].Key));
			for (int i = 0; i < arraySize; i++)
			{
				int intensity = binToIntensityMap[0].Value;
				int currentBin = binToIntensityMap[0].Key;

				// the intensities will always be positive integers
				tic += intensity;
				if (bpi < intensity)
				{
					bpi = intensity;
					indexOfMaxIntensity = i;
				}

				if (i != 0 && currentBin != previousBin + 1)
				{
					// since the bin numbers are not continuous, add a negative index to the array
					// and in some cases we have to add the offset from the previous index
					rlzeDataList.Add(previousBin - currentBin + 1);
				}

				rlzeDataList.Add(intensity);

				previousBin = currentBin;
			}

			// Compress intensities
			int nonZeroCount = 0;

			var nrlze = rlzeDataList.Count;
			int[] runLengthZeroEncodedData = rlzeDataList.ToArray();

			var compressedData = new byte[nrlze * dataTypeSize * 5];
			if (nrlze > 0)
			{
				var byteBuffer = new byte[nrlze * dataTypeSize];
				Buffer.BlockCopy(runLengthZeroEncodedData, 0, byteBuffer, 0, nrlze * dataTypeSize);
				nonZeroCount = LZFCompressionUtil.Compress(
					ref byteBuffer,
					nrlze * dataTypeSize,
					ref compressedData,
					compressedData.Length);
			}

			if (nonZeroCount != 0)
			{
				var spectra = new byte[nonZeroCount];
				Array.Copy(compressedData, spectra, nonZeroCount);

				InsertScanStoreBytes(frameParameters, scanNum, binWidth, indexOfMaxIntensity, nonZeroCount, bpi, tic, spectra);
			}

			return nonZeroCount;

		}

		/// <summary>
		/// This method takes in a list of bin numbers and intensities and converts them to a run length encoded array
		/// which is later compressed at the byte level for reduced size
		///   Used by the IonMobility data acquisition software in ADC_Acqiris_AP240.cs
		///   Used by the UIMF_DataViewer in DataViewer.cs
		/// </summary>
		/// <param name="frameParameters">
		/// </param>
		/// <param name="scanNum">
		/// </param>
		/// <param name="bins">
		/// </param>
		/// <param name="intensities">
		/// </param>
		/// <param name="binWidth">
		/// </param>
		/// <param name="timeOffset">
		/// </param>
		/// <returns>
		/// Non-zero data count<see cref="int"/>.
		/// </returns>
		[Obsolete("Superseded by InsertScan with: List<KeyValuePair<int, int>> binToIntensityMap, double binWidth")]
		public int InsertScan(
			FrameParameters frameParameters,
			int scanNum,
			int[] bins,
			int[] intensities,
			double binWidth,
			int timeOffset)
		{
			if (bins == null || intensities == null || bins.Length == 0 || intensities.Length == 0 ||
				bins.Length != intensities.Length)
			{
				return 0;
			}

			var binToIntensityMap = new List<KeyValuePair<int, int>>();

			int arraySize = intensities.Length;
			for (int i = 0; i < arraySize; i++)
			{
				binToIntensityMap.Add(new KeyValuePair<int, int>(bins[i], intensities[i]));
			}

			int nonZeroCount = InsertScan(frameParameters, scanNum, binToIntensityMap, binWidth, timeOffset);

			return nonZeroCount;

		}

		/// <summary>
		/// Insert a scan using a list of bins and a list of intensities
		/// </summary>
		/// <param name="frameParameters">
		/// </param>
		/// <param name="scanNum">
		/// </param>
		/// <param name="bins">
		/// </param>
		/// <param name="intensities">
		/// </param>
		/// <param name="binWidth">
		/// </param>
		/// <param name="timeOffset">
		/// </param>
		/// <returns>
		/// Non-zero data count<see cref="int"/>.
		/// </returns>
		[Obsolete("Superseded by InsertScan with: int[] intensities, double binWidth; alternatively use InsertScan with: List<KeyValuePair<int, int>> binToIntensityMap")]
		public int InsertScan(
			FrameParameters frameParameters,
			int scanNum,
			List<int> bins,
			List<int> intensities,
			double binWidth,
			int timeOffset)
		{			
			if (bins == null || intensities == null || bins.Count == 0 || intensities.Count == 0 ||
				bins.Count != intensities.Count)
			{
				return 0;
			}

			var binToIntensityMap = new List<KeyValuePair<int, int>>();

			int arraySize = intensities.Count;
			for (int i = 0; i < arraySize; i++)
			{
				binToIntensityMap.Add(new KeyValuePair<int, int>(bins[i], intensities[i]));
			}

			int nonZeroCount = InsertScan(frameParameters, scanNum, binToIntensityMap, binWidth, timeOffset);

			return nonZeroCount;
		}

		/// <summary>
		/// Insert a new scan using an array of intensities (as ints) and binWidth
		/// </summary>		
		/// <returns>
		/// The size of the compressed archive in the output buffer<see cref="int"/>.
		/// </returns>
		[Obsolete("Use InsertScan with: frameParameters, scanNum, intensities, and binWidth")]
		public int InsertScan(
			FrameParameters frameParameters,
			int scanNum,
			int nonZeroCountIgnored,			 // Ignored, since LZFCompressionUtil.Compress reports this
			int[] intensities,
			double binWidth)
		{
			return InsertScan(frameParameters, scanNum, intensities, binWidth);
		}

		/// <summary>
		/// Insert a new scan using an array of intensities (as doubles) and binWidth
		/// </summary>		
		/// <returns>
		/// The size of the compressed archive in the output buffer<see cref="int"/>.
		/// </returns>		
		[Obsolete("Use InsertScan with: frameParameters, scanNum, intensities, and binWidth")]
		public int InsertScan(
			FrameParameters frameParameters,
			int scanNum,
			int nonZeroCountIgnored,			 // Ignored, since LZFCompressionUtil.Compress reports this
			double[] intensities,
			double binWidth)
		{
			return InsertScan(frameParameters, scanNum, intensities, binWidth);
		}

		/// <summary>
		/// </summary>
		/// <param name="scanNum">
		/// </param>
		/// <param name="ms_level">
		/// </param>
		/// <returns>
		/// Always returns 0<see cref="int"/>.
		/// </returns>
		public int InsertScanParameters(int scanNum, int ms_level)
		{
			this.insertScanAddScanParameters(scanNum, ms_level);

			this.m_dbCommandPrepareInsertScanParameters.ExecuteNonQuery();
			this.m_dbCommandPrepareInsertScanParameters.Parameters.Clear();

			return 0;
		}

		/// <summary>
		/// Post a new log entry to table Log_Entries
		/// </summary>
		/// <param name="EntryType">
		/// Log entry type (typically Normal, Error, or Warning)
		/// </param>
		/// <param name="Message">
		/// Log message
		/// </param>
		/// <param name="PostedBy">
		/// Process or application posting the log message
		/// </param>
		/// <remarks>
		/// The Log_Entries table will be created if it doesn't exist
		/// </remarks>
		public void PostLogEntry(string EntryType, string Message, string PostedBy)
		{
			PostLogEntry(this.m_dbConnection, EntryType, Message, PostedBy);
		}

		/// <summary>
		/// </summary>
		/// <param name="frameNumber">
		/// </param>
		/// <param name="slope">
		/// </param>
		/// <param name="intercept">
		/// </param>
		public void UpdateCalibrationCoefficients(int frameNumber, float slope, float intercept)
		{
			this.m_dbCommandUimf = this.m_dbConnection.CreateCommand();
			this.m_dbCommandUimf.CommandText = "UPDATE Frame_Parameters SET CalibrationSlope = " + slope
											   + ", CalibrationIntercept = " + intercept + " WHERE FrameNum = " + frameNumber;

			this.m_dbCommandUimf.ExecuteNonQuery();
			this.m_dbCommandUimf.Dispose();
		}

		/// <summary>
		/// </summary>
		/// <param name="frameNumber">
		/// </param>
		/// <param name="parameterName">
		/// </param>
		/// <param name="parameterValue">
		/// </param>
		public void UpdateFrameParameter(int frameNumber, string parameterName, string parameterValue)
		{
			// Make sure the Frame_Parameters table has all of the required columns
			this.ValidateFrameParameterColumns();

			this.m_dbCommandUimf = this.m_dbConnection.CreateCommand();
			this.m_dbCommandUimf.CommandText = "UPDATE Frame_Parameters SET " + parameterName + " = " + parameterValue
											   + " WHERE FrameNum = " + frameNumber;
			this.m_dbCommandUimf.ExecuteNonQuery();
			this.m_dbCommandUimf.Dispose();
		}

		/// <summary>
		/// </summary>
		/// <param name="frameNumber">
		/// </param>
		/// <param name="parameters">
		/// </param>
		/// <param name="values">
		/// </param>
		public void UpdateFrameParameters(int frameNumber, List<string> parameters, List<string> values)
		{
			// Make sure the Frame_Parameters table has all of the required columns
			this.ValidateFrameParameterColumns();

			var commandText = new StringBuilder("UPDATE Frame_Parameters SET ");
			for (int i = 0; i < parameters.Count - 1; i++)
			{
				commandText.Append(parameters[i] + "=" + values[i] + ",");
			}

			commandText.Append(parameters[parameters.Count - 1] + "=" + values[values.Count - 1]);

			this.m_dbCommandUimf = this.m_dbConnection.CreateCommand();
			this.m_dbCommandUimf.CommandText = commandText + " WHERE FrameNum = " + frameNumber;

			// Console.WriteLine(m_dbCommandUimf.CommandText);
			this.m_dbCommandUimf.ExecuteNonQuery();
			this.m_dbCommandUimf.Dispose();
		}

		/// <summary>
		/// Updates the scan count for the given frame
		/// </summary>
		/// <param name="frameNum">
		/// The frame number to update
		/// </param>
		/// <param name="NumScans">
		/// The new scan count
		/// </param>
		public void UpdateFrameScanCount(int frameNum, int NumScans)
		{
			this.UpdateFrameParameter(frameNum, "Scans", NumScans.ToString());
		}

		/// <summary>
		/// This function updates the frame type to 1, 2, 2, 2, 1, 2, 2, 2, etc. for the specified frame range
		/// It is used in the nunit tests
		/// </summary>
		/// <param name="startFrameNum">
		/// The start Frame Num.
		/// </param>
		/// <param name="endFrameNum">
		/// The end Frame Num.
		/// </param>
		public void UpdateFrameType(int startFrameNum, int endFrameNum)
		{
			this.m_dbCommandUimf = this.m_dbConnection.CreateCommand();
			this.m_dbCommandUimf.CommandText = "UPDATE FRAME_PARAMETERS SET FRAMETYPE= :FRAMETYPE WHERE FRAMENUM = :FRAMENUM";
			this.m_dbCommandUimf.Prepare();

			for (int i = startFrameNum; i <= endFrameNum; i++)
			{
				int frameType = i % 4 == 0 ? 1 : 2;
				this.m_dbCommandUimf.Parameters.Add(new SQLiteParameter("FRAMETYPE", frameType));
				this.m_dbCommandUimf.Parameters.Add(new SQLiteParameter("FRAMENUM", i));
				this.m_dbCommandUimf.ExecuteNonQuery();
				this.m_dbCommandUimf.Parameters.Clear();
			}
		}

		/// <summary>
		/// Assures that NumFrames in the Global_Parameters table match the number of rows in the Frame_Parameters table
		/// </summary>
		public void UpdateGlobalFrameCount()
		{
			this.m_dbCommandUimf = this.m_dbConnection.CreateCommand();

			this.m_dbCommandUimf.CommandText = "SELECT Count(*) FROM Frame_Parameters";
			object frameCount = this.m_dbCommandUimf.ExecuteScalar();
			this.m_dbCommandUimf.Dispose();

			if (frameCount != null)
			{
				this.UpdateGlobalParameter("NumFrames", frameCount.ToString());
			}
		}

		/// <summary>
		/// </summary>
		/// <param name="parameterName">
		/// </param>
		/// <param name="parameterValue">
		/// </param>
		public void UpdateGlobalParameter(string parameterName, string parameterValue)
		{
			var lstFields = GetGlobalParametersFields();

			// Validate the field name
			Tuple<string, string, string> fieldMatch = null;
			foreach (var fieldInfo in lstFields)
			{
				if (fieldInfo.Item1 == parameterName)
				{
					fieldMatch = fieldInfo;
					break;
				}
			}

			if (fieldMatch == null)
				throw new Exception("Invalid global parameter name, " + parameterName);

			if (fieldMatch.Item3 == "string")
				parameterValue = "'" + parameterValue + "'";

			this.m_dbCommandUimf = this.m_dbConnection.CreateCommand();
			this.m_dbCommandUimf.CommandText = "UPDATE Global_Parameters SET " + parameterName + " = "
											   + parameterValue;
			this.m_dbCommandUimf.ExecuteNonQuery();
			this.m_dbCommandUimf.Dispose();
		}

		/// <summary>
		/// </summary>
		/// <param name="tableName">
		/// </param>
		/// <param name="fileBytesAsBuffer">
		/// </param>
		/// <returns>
		/// Always returns true<see cref="bool"/>.
		/// </returns>
		public bool WriteFileToTable(string tableName, byte[] fileBytesAsBuffer)
		{
			this.m_dbCommandUimf = this.m_dbConnection.CreateCommand();
			try
			{
				if (!DataReader.TableExists(this.m_dbConnection, tableName))
				{
					// Create the table
					this.m_dbCommandUimf.CommandText = "CREATE TABLE " + tableName + " (FileText BLOB);";
					this.m_dbCommandUimf.ExecuteNonQuery();
				}
				else
				{
					// Delete the data currently in the table
					this.m_dbCommandUimf.CommandText = "DELETE FROM " + tableName + ";";
					this.m_dbCommandUimf.ExecuteNonQuery();
				}

				this.m_dbCommandUimf.CommandText = "INSERT INTO " + tableName + " VALUES (:Buffer);";
				this.m_dbCommandUimf.Prepare();

				this.m_dbCommandUimf.Parameters.Add(new SQLiteParameter(":Buffer", fileBytesAsBuffer));

				this.m_dbCommandUimf.ExecuteNonQuery();
			}
			finally
			{
				this.m_dbCommandUimf.Parameters.Clear();
				this.m_dbCommandUimf.Dispose();
			}

			return true;
		}

		#endregion

		#region Methods

		/// <summary>
		/// Assures that certain columns are present in the Frame_Parameters table
		/// </summary>
		protected void ValidateFrameParameterColumns()
		{
			if (!this.m_FrameParameterColumnsVerified)
			{
				if (!DataReader.TableHasColumn(this.m_dbConnection, "Frame_Parameters", "Decoded"))
				{
					this.AddFrameParameter("Decoded", "INT", 0);
				}

				this.m_FrameParameterColumnsVerified = true;
			}
		}

		/// <summary>
		/// </summary>
		/// <param name="frag">
		/// </param>
		/// <returns>
		/// Byte array<see cref="byte[]"/>.
		/// </returns>
		private static byte[] ConvertToBlob(double[] frag)
		{
			// convert the fragmentation profile into an array of bytes
			int length_blob = frag.Length;
			var blob_values = new byte[length_blob * 8];

			Buffer.BlockCopy(frag, 0, blob_values, 0, length_blob * 8);

			return blob_values;
		}

		/// <summary>
		/// Creates the table creation DDL using the table name and field info
		/// </summary>
		/// <param name="tableName">
		/// Table name
		/// </param>
		/// <param name="lstFields">
		/// List of Tuples where Item1 is FieldName, Item2 is Sql data type, and Item3 is .NET data type
		/// </param>
		/// <returns></returns>
		private string GetCreateTableSql(string tableName, IList<Tuple<string, string, string>> lstFields)
		{
			var sbSql = new StringBuilder("CREATE TABLE " + tableName + " ( ");

			for (int i = 0; i < lstFields.Count; i++)
			{
				sbSql.Append(lstFields[i].Item1 + " " + lstFields[i].Item2);

				if (i < lstFields.Count - 1)
				{
					sbSql.Append(", ");
				}
			}
			sbSql.Append(");");

			return sbSql.ToString();
		}

		/// <summary>
		/// Gets the field names for the Frame_Parameters table
		/// </summary>
		/// <returns>
		/// List of Tuples where Item1 is FieldName, Item2 is Sql data type, and Item3 is .NET data type
		/// </returns>
		private List<Tuple<string, string, string>> GetFrameParametersFields()
		{

			var lstFields = new List<Tuple<string, string, string>>
			{
				Tuple.Create("FrameNum", "INTEGER PRIMARY KEY", "int"),
				Tuple.Create("StartTime", "DOUBLE", "double"),
				Tuple.Create("Duration", "DOUBLE", "double"),
				Tuple.Create("Accumulations", "SMALLINT", "short"),
				Tuple.Create("FrameType", "SMALLINT", "short"),
				Tuple.Create("Scans", "INTEGER", "int"),
				Tuple.Create("IMFProfile", "TEXT", "string"),
				Tuple.Create("TOFLosses", "DOUBLE", "double"),
				Tuple.Create("AverageTOFLength", "DOUBLE NOT NULL", "double"),
				Tuple.Create("CalibrationSlope", "DOUBLE", "double"),
				Tuple.Create("CalibrationIntercept", "DOUBLE", "double"),
				Tuple.Create("a2", "DOUBLE", "double"),
				Tuple.Create("b2", "DOUBLE", "double"),
				Tuple.Create("c2", "DOUBLE", "double"),
				Tuple.Create("d2", "DOUBLE", "double"),
				Tuple.Create("e2", "DOUBLE", "double"),
				Tuple.Create("f2", "DOUBLE", "double"),
				Tuple.Create("Temperature", "DOUBLE", "double"),
				Tuple.Create("voltHVRack1", "DOUBLE", "double"),
				Tuple.Create("voltHVRack2", "DOUBLE", "double"),
				Tuple.Create("voltHVRack3", "DOUBLE", "double"),
				Tuple.Create("voltHVRack4", "DOUBLE", "double"),
				Tuple.Create("voltCapInlet", "DOUBLE", "double"),
				Tuple.Create("voltEntranceHPFIn", "DOUBLE", "double"),
				Tuple.Create("voltEntranceHPFOut", "DOUBLE", "double"),
				Tuple.Create("voltEntranceCondLmt", "DOUBLE", "double"),
				Tuple.Create("voltTrapOut", "DOUBLE", "double"),
				Tuple.Create("voltTrapIn", "DOUBLE", "double"),
				Tuple.Create("voltJetDist", "DOUBLE", "double"),
				Tuple.Create("voltQuad1", "DOUBLE", "double"),
				Tuple.Create("voltCond1", "DOUBLE", "double"),
				Tuple.Create("voltQuad2", "DOUBLE", "double"),
				Tuple.Create("voltCond2", "DOUBLE", "double"),
				Tuple.Create("voltIMSOut", "DOUBLE", "double"),
				Tuple.Create("voltExitHPFIn", "DOUBLE", "double"),
				Tuple.Create("voltExitHPFOut", "DOUBLE", "double"),
				Tuple.Create("voltExitCondLmt", "DOUBLE", "double"),
				Tuple.Create("PressureFront", "DOUBLE", "double"),
				Tuple.Create("PressureBack", "DOUBLE", "double"),
				Tuple.Create("MPBitOrder", "TINYINT", "short"),
				Tuple.Create("FragmentationProfile", "BLOB", "object"),
				Tuple.Create("HighPressureFunnelPressure", "DOUBLE", "double"),
				Tuple.Create("IonFunnelTrapPressure", "DOUBLE ", "double"),
				Tuple.Create("RearIonFunnelPressure", "DOUBLE", "double"),
				Tuple.Create("QuadrupolePressure", "DOUBLE", "double"),
				Tuple.Create("ESIVoltage", "DOUBLE", "double"),
				Tuple.Create("FloatVoltage", "DOUBLE", "double"),
				Tuple.Create("CalibrationDone", "INTEGER", "int"),
				Tuple.Create("Decoded", "INTEGER", "int")
			};

			return lstFields;

		}

		/// <summary>
		/// Gets the field names for the Frame_Scans table
		/// </summary>
		/// <param name="dataType">
		/// double, float, or int
		/// </param>
		/// <returns>
		/// List of Tuples where Item1 is FieldName, Item2 is Sql data type, and Item3 is .NET data type
		/// </returns>
		private List<Tuple<string, string, string>> GetFrameScansFields(string dataType)
		{

			string sqlDataType;
			string dotNetDataType;

			if (string.Equals(dataType, "double"))
			{
				sqlDataType = "DOUBLE";
				dotNetDataType = "double";
			}
			else if (string.Equals(dataType, "float"))
			{
				sqlDataType = "FLOAT";
				dotNetDataType = "float";
			}
			else
			{
				// Assume 32-bit int
				sqlDataType = "INTEGER";
				dotNetDataType = "int";
			}

			var lstFields = new List<Tuple<string, string, string>>
			{
				Tuple.Create("FrameNum", "INTEGER NOT NULL", "int"),
				Tuple.Create("ScanNum", "SMALLINT NOT NULL", "short"),
				Tuple.Create("NonZeroCount", "INTEGER NOT NULL", "int"),
				Tuple.Create("BPI", sqlDataType + " NOT NULL", dotNetDataType),
				Tuple.Create("BPI_MZ", "DOUBLE NOT NULL", "double"),
				Tuple.Create("TIC", sqlDataType + " NOT NULL", dotNetDataType),
				Tuple.Create("Intensities", "BLOB", "object")
			};

			return lstFields;

		}

		/// <summary>
		/// Gets the field names for the Global_Parameters table
		/// </summary>
		/// <returns>
		/// List of Tuples where Item1 is FieldName, Item2 is Sql data type, and Item3 is .NET data type
		/// </returns>
		private List<Tuple<string, string, string>> GetGlobalParametersFields()
		{

			var lstFields = new List<Tuple<string, string, string>>
			{
				Tuple.Create("DateStarted", "TEXT", "string"),
				Tuple.Create("NumFrames", "INTEGER NOT NULL", "int"),
				Tuple.Create("TimeOffset", "INTEGER NOT NULL", "int"),
				Tuple.Create("BinWidth", "DOUBLE NOT NULL", "double"),
				Tuple.Create("Bins", "INTEGER NOT NULL", "int"),
				Tuple.Create("TOFCorrectionTime", "FLOAT NOT NULL", "float"),
				Tuple.Create("FrameDataBlobVersion", "FLOAT NOT NULL", "float"),
				Tuple.Create("ScanDataBlobVersion", "FLOAT NOT NULL", "float"),
				Tuple.Create("TOFIntensityType", "TEXT NOT NULL", "string"),
				Tuple.Create("DatasetType", "TEXT", "string"),
				Tuple.Create("Prescan_TOFPulses", "INTEGER", "int"),
				Tuple.Create("Prescan_Accumulations", "INTEGER", "int"),
				Tuple.Create("Prescan_TICThreshold", "INTEGER", "int"),
				Tuple.Create("Prescan_Continuous", "BOOLEAN", "bool"),
				Tuple.Create("Prescan_Profile", "TEXT", "string"),
				Tuple.Create("Instrument_Name", "TEXT", "string")
			};

			return lstFields;
		}

		/// <summary>
		/// </summary>
		/// <param name="frameNumber">
		/// </param>
		/// <param name="scanNum">
		/// </param>
		/// <param name="nonZeroCount">
		/// </param>
		/// <param name="bpi">
		/// </param>
		/// <param name="bpiMz">
		/// </param>
		/// <param name="tic">
		/// </param>
		/// <param name="spectraRecord">
		/// </param>
		private void InsertScanAddParameters(
			int frameNumber,
			int scanNum,
			int nonZeroCount,
			int bpi,
			double bpiMz,
			int tic,
			byte[] spectraRecord)
		{
			this.m_dbCommandPrepareInsertScan.Parameters.Add(new SQLiteParameter("FrameNum", frameNumber.ToString()));
			this.m_dbCommandPrepareInsertScan.Parameters.Add(new SQLiteParameter("ScanNum", scanNum.ToString()));
			this.m_dbCommandPrepareInsertScan.Parameters.Add(new SQLiteParameter("NonZeroCount", nonZeroCount.ToString()));
			this.m_dbCommandPrepareInsertScan.Parameters.Add(new SQLiteParameter("BPI", bpi.ToString()));
			this.m_dbCommandPrepareInsertScan.Parameters.Add(new SQLiteParameter("BPI_MZ", bpiMz.ToString()));
			this.m_dbCommandPrepareInsertScan.Parameters.Add(new SQLiteParameter("TIC", tic.ToString()));
			this.m_dbCommandPrepareInsertScan.Parameters.Add(new SQLiteParameter("Intensities", spectraRecord));
		}

		/// <summary>
		/// Create command for inserting frames
		/// </summary>
		private void PrepareInsertFrame()
		{
			this.m_dbCommandPrepareInsertFrame = this.m_dbConnection.CreateCommand();

			string voltEntranceHPFInColName;
			string voltEntranceHPFOutColName;

			string voltExitHPFInColName;
			string voltExitHPFOutColName;

			if (DataReader.ColumnExists(this.m_dbConnection, "Frame_Parameters", "voltEntranceHPFIn"))
			{
				voltEntranceHPFInColName = "voltEntranceHPFIn";
				voltEntranceHPFOutColName = "voltEntranceHPFOut";
			}
			else
			{
				voltEntranceHPFInColName = "voltEntranceIFTIn";
				voltEntranceHPFOutColName = "voltEntranceIFTOut";
			}

			if (DataReader.ColumnExists(this.m_dbConnection, "Frame_Parameters", "voltExitHPFIn"))
			{
				voltExitHPFInColName = "voltExitHPFIn";
				voltExitHPFOutColName = "voltExitHPFOut";
			}
			else
			{
				voltExitHPFInColName = "voltExitIFTIn";
				voltExitHPFOutColName = "voltExitIFTOut";
			}

			string cmd =
				"INSERT INTO Frame_Parameters (FrameNum, StartTime, Duration, Accumulations, FrameType, Scans, IMFProfile, TOFLosses,"
				  + "AverageTOFLength, CalibrationSlope, CalibrationIntercept,a2, b2, c2, d2, e2, f2, Temperature, voltHVRack1, voltHVRack2, voltHVRack3, voltHVRack4, "
				  + "voltCapInlet, " + voltEntranceHPFInColName + ", " + voltEntranceHPFOutColName + ", "
				  + "voltEntranceCondLmt, " + "voltTrapOut, voltTrapIn, voltJetDist, voltQuad1, voltCond1, voltQuad2, voltCond2, "
				  + "voltIMSOut, " + voltExitHPFInColName + ", " + voltExitHPFOutColName + ", "
				  + "voltExitCondLmt, PressureFront, PressureBack, MPBitOrder, FragmentationProfile, HighPressureFunnelPressure, IonFunnelTrapPressure, "
				  + "RearIonFunnelPressure, QuadrupolePressure, ESIVoltage, FloatVoltage, CalibrationDone, Decoded)"
				+ "VALUES (:FrameNum, :StartTime, :Duration, :Accumulations, :FrameType,:Scans,:IMFProfile,:TOFLosses,"
				  + ":AverageTOFLength,:CalibrationSlope,:CalibrationIntercept,:a2,:b2,:c2,:d2,:e2,:f2,:Temperature,:voltHVRack1,:voltHVRack2,:voltHVRack3,:voltHVRack4, "
				  + ":voltCapInlet,:" + voltEntranceHPFInColName + ",:" + voltEntranceHPFOutColName + ","
				  + ":voltEntranceCondLmt,:voltTrapOut,:voltTrapIn,:voltJetDist,:voltQuad1,:voltCond1,:voltQuad2,:voltCond2,"
				  + ":voltIMSOut,:" + voltExitHPFInColName + ",:" + voltExitHPFOutColName + ",:voltExitCondLmt, "
				  + ":PressureFront,:PressureBack,:MPBitOrder,:FragmentationProfile, " + ":HPPressure, :IPTrapPressure, "
				  + ":RIFunnelPressure, :QuadPressure, :ESIVoltage, :FloatVoltage, :CalibrationDone, :Decoded);";

			this.m_dbCommandPrepareInsertFrame.CommandText = cmd;
			this.m_dbCommandPrepareInsertFrame.Prepare();
		}

		/// <summary>
		/// Create command for inserting scans
		/// </summary>
		private void PrepareInsertScan()
		{
			// This function should be called before looping through each frame and scan
			this.m_dbCommandPrepareInsertScan = this.m_dbConnection.CreateCommand();
			this.m_dbCommandPrepareInsertScan.CommandText =
				"INSERT INTO Frame_Scans (FrameNum, ScanNum, NonZeroCount, BPI, BPI_MZ, TIC, Intensities) "
				+ "VALUES(?,?,?,?,?,?,?);";
			this.m_dbCommandPrepareInsertScan.Prepare();

			if (this.m_isScanParameterTable)
			{
				this.m_dbCommandPrepareInsertScanParameters = this.m_dbConnection.CreateCommand();
				this.m_dbCommandPrepareInsertScanParameters.CommandText =
					"INSERT INTO Scan_Parameters (ScanNum, MS_Level) VALUES (:ScanNum, :MS_Level);";

				this.m_dbCommandPrepareInsertScanParameters.Prepare();
			}
		}

		/// <summary>
		/// Begin a transaction
		/// </summary>
		private void TransactionBegin()
		{
			this.m_dbCommandUimf = this.m_dbConnection.CreateCommand();
			this.m_dbCommandUimf.CommandText = "PRAGMA synchronous=0;BEGIN TRANSACTION;";
			this.m_dbCommandUimf.ExecuteNonQuery();
		}

		/// <summary>
		/// Commit a transaction
		/// </summary>
		private void TransactionCommit()
		{
			this.m_dbCommandUimf = this.m_dbConnection.CreateCommand();
			this.m_dbCommandUimf.CommandText = "END TRANSACTION;PRAGMA synchronous=1;";
			this.m_dbCommandUimf.ExecuteNonQuery();
		}

		/// <summary>
		/// Convert bin number to m/z value
		/// </summary>
		/// <param name="binNumber">
		/// </param>
		/// <param name="binWidth">
		/// </param>
		/// <param name="frameParameters">
		/// </param>		
		/// <returns>
		/// m/z<see cref="double"/>.
		/// </returns>
		public double ConvertBinToMz(int binNumber, double binWidth, FrameParameters frameParameters)
		{
			// mz = (k * (t-t0))^2
			double t = binNumber * binWidth / 1000;
			double resMassErr = frameParameters.a2 * t + frameParameters.b2 * Math.Pow(t, 3)
								+ frameParameters.c2 * Math.Pow(t, 5) + frameParameters.d2 * Math.Pow(t, 7)
								+ frameParameters.e2 * Math.Pow(t, 9) + frameParameters.f2 * Math.Pow(t, 11);
			var mz =
				frameParameters.CalibrationSlope *
				((t - (double)this.m_globalParameters.TOFCorrectionTime / 1000 - frameParameters.CalibrationIntercept));
			mz = (mz * mz) + resMassErr;
			return mz;
		}

		/// <summary>
		/// </summary>
		/// <param name="scan_number">
		/// </param>
		/// <param name="MS_Level">
		/// </param>
		private void insertScanAddScanParameters(int scan_number, int MS_Level)
		{
			this.m_dbCommandPrepareInsertScanParameters.Parameters.Add(new SQLiteParameter("ScanNum", scan_number.ToString()));
			this.m_dbCommandPrepareInsertScanParameters.Parameters.Add(new SQLiteParameter("MS_Level", MS_Level.ToString()));
		}

		#endregion
	}
}