using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SQLite;

namespace UIMFLibrary
{
    internal class FrameNumShifter
    {

        #region "Events"

        /// <summary>
        /// Error event
        /// </summary>
        public event EventHandler<FrameNumShiftEventArgs> FrameShiftEvent;

        #endregion

        #region "Properties"

        /// <summary>
        /// Connection to the database
        /// </summary>
        private SQLiteConnection DBConnection { get; }

        /// <summary>
        /// True if the UIMF file has the Frame_Parameters table
        /// </summary>
        private bool HasLegacyParameterTables { get; }

        #endregion

        /// <summary>
        /// Constructor
        /// </summary>
        public FrameNumShifter(SQLiteConnection dbConnection, bool hasLegacyParameterTables)
        {
            DBConnection = dbConnection;
            HasLegacyParameterTables = hasLegacyParameterTables;
        }

        private static string GetFrameRanges(IReadOnlyList<int> frameNums)
        {

            var frameRanges = new List<string>();
            var startFrame = frameNums[0];

            for (var i = 0; i < frameNums.Count - 1; i++)
            {
                if (frameNums[i] - frameNums[i + 1] <= 1)
                    continue;

                frameRanges.Add(GetFrameRangeDescription(startFrame, frameNums[i]));
                startFrame = frameNums[i + 1];
            }

            frameRanges.Add(GetFrameRangeDescription(startFrame, frameNums[frameNums.Count - 1]));

            return string.Join(", ", frameRanges);
        }

        private static string GetFrameRangeDescription(int startFrame, int endFrame)
        {
            if (startFrame == endFrame)
                return startFrame.ToString();

            return startFrame + "-" + endFrame;
        }

        /// <summary>
        /// Renumber frames so that the first frame is frame 1 and to assure that there are no gaps in frame numbers
        /// </summary>
        /// <remarks>This method is used by the UIMFDemultiplexer when the first frame to process is not frame 1</remarks>
        public void RenumberFrames()
        {
            using (var dbCommand = DBConnection.CreateCommand())
            {

                // Obtain a list of the current frame numbers
                var frameNums = new List<int>();

                dbCommand.CommandText = "SELECT DISTINCT FrameNum FROM Frame_Params ORDER BY FrameNum;";
                using (var reader = dbCommand.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var frameNum = reader.GetInt32(0);
                        frameNums.Add(frameNum);
                    }
                }

                if (frameNums.Count == 0)
                {
                    // Nothing to do
                    return;
                }

                // Determine the first frame number that is a calibration scan
                var firstCalibrationFrame = 0;

                dbCommand.CommandText = string.Format(
                    " SELECT FrameNum " +
                    " FROM frame_params " +
                    " WHERE ParamID = {0} AND ParamValue = {1} " +
                    " ORDER BY FrameNum " +
                    " LIMIT 1;",
                    (int)FrameParamKeyType.FrameType,
                    (int)UIMFData.FrameType.Calibration);

                using (var reader = dbCommand.ExecuteReader())
                {
                    if (reader.Read())
                    {
                        firstCalibrationFrame = reader.GetInt32(0);
                    }
                }

                // Dictionary mapping old frame number to new frame number
                var frameNumMapping = new Dictionary<int, int>();
                var nextFrameNumber = 1;

                var renumberRequired = false;
                var needToAddFirstCalibrationFrame = false;

                for (var i = 0; i < frameNums.Count; i++)
                {
                    var frameNum = frameNums[i];

                    if (i == 0 && firstCalibrationFrame > 0 && frameNum == firstCalibrationFrame)
                    {
                        // The first frame in the frames we want to keep is the calibration frame
                        // This frame likely has fewer scans than the other frames in the UIMF file
                        // The legacy UIMF Viewer caps the number of scans to display to the number of scans in the first frame
                        // Thus, we cannot have the first frame be the calibration frame and will instead add it later
                        needToAddFirstCalibrationFrame = true;
                        continue;
                    }

                    if (frameNum != nextFrameNumber)
                        renumberRequired = true;

                    frameNumMapping.Add(frameNum, nextFrameNumber);
                    nextFrameNumber++;

                    if (needToAddFirstCalibrationFrame && frameNum > firstCalibrationFrame)
                    {
                        // Add the calibration frame now
                        renumberRequired = true;
                        frameNumMapping.Add(firstCalibrationFrame, nextFrameNumber);
                        nextFrameNumber++;

                        needToAddFirstCalibrationFrame = false;
                    }

                }

                if (!renumberRequired)
                {
                    // Nothing to do
                    return;
                }

                var frameNumsInBatch = new List<int> {
                    frameNums[0]
                };

                var deltaForBatch = frameNums[0] - frameNumMapping[frameNums[0]];

                for (var i = 1; i < frameNums.Count; i++)
                {
                    var thisFrame = frameNums[i];
                    var frameNumNew = frameNumMapping[thisFrame];
                    var delta = thisFrame - frameNumNew;

                    if (delta == deltaForBatch)
                    {
                        frameNumsInBatch.Add(thisFrame);
                        continue;
                    }

                    ShiftFramesInBatch(dbCommand, frameNumsInBatch, deltaForBatch);
                    frameNumsInBatch.Clear();

                    frameNumsInBatch.Add(thisFrame);
                    deltaForBatch = delta;
                }

                ShiftFramesInBatch(dbCommand, frameNumsInBatch, deltaForBatch);

            }

        }

        /// <summary>
        /// Shift the frame number for the frames in frameNums, shifting down by decrementAmount
        /// </summary>
        /// <param name="dbCommand"></param>
        /// <param name="frameNums"></param>
        /// <param name="decrementAmount"></param>
        /// <remarks>Used by RenumberFrames when adjusting frames to start at frame 1 and to not have any gaps</remarks>
        private void ShiftFramesInBatch(IDbCommand dbCommand, IReadOnlyList<int> frameNums, int decrementAmount)
        {
            if (frameNums.Count == 0)
                return;

            // Construct a comma-separated list of frame numbers
            var sFrameList = string.Join(",", frameNums);

            dbCommand.CommandText =
                " UPDATE Frame_Params " +
                " SET FrameNum = FrameNum - " + decrementAmount +
                " WHERE FrameNum IN (" + sFrameList + "); ";
            dbCommand.ExecuteNonQuery();

            dbCommand.CommandText =
                " UPDATE Frame_Scans " +
                " SET FrameNum = FrameNum - " + decrementAmount +
                " WHERE FrameNum IN (" + sFrameList + "); ";
            dbCommand.ExecuteNonQuery();

            if (HasLegacyParameterTables)
            {
                dbCommand.CommandText =
                    " UPDATE Frame_Parameters " +
                    " SET FrameNum = FrameNum - " + decrementAmount +
                    " WHERE FrameNum IN (" + sFrameList + "); ";
                dbCommand.ExecuteNonQuery();
            }

            var frameRanges = GetFrameRanges(frameNums);

            OnFramesShifted(decrementAmount, frameRanges);

        }

        private void OnFramesShifted(int decrementAmount, string frameRanges)
        {
            if (FrameShiftEvent != null)
            {
                FrameShiftEvent(this, new FrameNumShiftEventArgs(decrementAmount, frameRanges));
            }
            else
            {
                Console.WriteLine("Decremented frame number by {0} for frames {1}", decrementAmount, frameRanges);
            }

        }
    }
}
