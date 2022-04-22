using System;
using System.Collections.Generic;

namespace UIMFLibrary.UnitTests.Legacy
{
    // ReSharper disable CommentTypo

    // Ignore Spelling: Prescan, M/d/yyyy, hh:mm:ss tt

    // ReSharper restore CommentTypo

    /// <summary>
    /// Global parameters
    /// </summary>
    [Obsolete("This class has been superseded by the GlobalParams class")]
    internal class GlobalParameters
    {
        // Ignore Spelling: yyyy hh:mm:ss tt

        /// <summary>
        ///Width of TOF bins (in nanoseconds)
        /// </summary>
        public double BinWidth = 0.0;

        /// <summary>
        /// Total number of TOF bins in frame
        /// </summary>
        public int Bins = 0;

        /// <summary>
        /// Type of dataset (HMS/HMS/HMS-MSn)
        /// </summary>
        public string DatasetType;

        /// <summary>
        /// Date started.
        /// </summary>
        /// <remarks>
        /// Format has traditionally been M/d/yyyy hh:mm:ss tt
        /// For example, 6/4/2014 12:56:44 PM</remarks>
        public string DateStarted = "";

        /// <summary>
        /// Version of FrameDataBlob in T_Frame
        /// </summary>
        /// <remarks>Obsolete / never used</remarks>
        public float FrameDataBlobVersion = 0f;

        /// <summary>
        /// Instrument name.
        /// </summary>
        public string InstrumentName = "";

        /// <summary>
        /// Number of frames in dataset
        /// </summary>
        public int NumFrames = 0;

        /// <summary>
        /// Number of prescan accumulations
        /// </summary>
        public int Prescan_Accumulations = 0;

        /// <summary>
        /// Prescan Continuous flag
        /// </summary>
        public bool Prescan_Continuous = false;

        /// <summary>
        /// Prescan profile.
        /// </summary>
        /// <remarks>
        /// If continuous is true, set this to NULL;
        /// </remarks>
        public string Prescan_Profile = "";

        /// <summary>
        /// Prescan TIC threshold
        /// </summary>
        public int Prescan_TICThreshold = 0;

        /// <summary>
        /// Prescan TOF pulses
        /// </summary>
        /// <remarks>
        /// Tracks the maximum scan number in any frame
        /// </remarks>
        public int Prescan_TOFPulses = 0;

        /// <summary>
        /// Version of ScanInfoBlob in T_Frame
        /// </summary>
        /// <remarks>Obsolete / never used</remarks>
        public float ScanDataBlobVersion = 0f;

        /// <summary>
        /// TOF correction time.
        /// </summary>
        public float TOFCorrectionTime = 0f;

        /// <summary>
        /// Data type of intensity in each TOF record (ADC is int, TDC is short, FOLDED is float)
        /// </summary>
        public string TOFIntensityType = "";

        /// <summary>
        /// Time offset from 0. All bin numbers must be offset by this amount
        /// </summary>
        public int TimeOffset = 0;

        /// <summary>
        /// Create a Global parameter dictionary using a GlobalParameters class instance
        /// </summary>
        /// <param name="globalParameters"></param>
        /// <returns>Global parameter dictionary</returns>
        internal static Dictionary<GlobalParamKeyType, dynamic> ConvertGlobalParameters(GlobalParameters globalParameters)
        {
            var prescanContinuous = 0;

            if (globalParameters.Prescan_Continuous)
                prescanContinuous = 1;

            var globalParams = new Dictionary<GlobalParamKeyType, dynamic>
            {
                {GlobalParamKeyType.InstrumentName, globalParameters.InstrumentName},
                {GlobalParamKeyType.DateStarted, globalParameters.DateStarted},
                {GlobalParamKeyType.NumFrames, globalParameters.NumFrames},
                {GlobalParamKeyType.TimeOffset, globalParameters.TimeOffset},
                {GlobalParamKeyType.BinWidth, globalParameters.BinWidth},
                {GlobalParamKeyType.Bins, globalParameters.Bins},
                {GlobalParamKeyType.TOFCorrectionTime, globalParameters.TOFCorrectionTime},
                // Obsolete: {GlobalParamKeyType.FrameDataBlobVersion, globalParameters.FrameDataBlobVersion},
                // Obsolete: {GlobalParamKeyType.ScanDataBlobVersion, globalParameters.ScanDataBlobVersion},
                {GlobalParamKeyType.TOFIntensityType, globalParameters.TOFIntensityType},
                {GlobalParamKeyType.DatasetType, globalParameters.DatasetType},
                {GlobalParamKeyType.PrescanTOFPulses, globalParameters.Prescan_TOFPulses},
                {GlobalParamKeyType.PrescanAccumulations, globalParameters.Prescan_Accumulations},
                {GlobalParamKeyType.PrescanTICThreshold, globalParameters.Prescan_TICThreshold},
                {GlobalParamKeyType.PrescanContinuous, prescanContinuous},
                {GlobalParamKeyType.PrescanProfile, globalParameters.Prescan_Profile}
            };

            return globalParams;
        }

        /// <summary>
        /// Convert a Global parameter dictionary to an instance of the <see cref="GlobalParams"/> class
        /// </summary>
        /// <param name="GlobalParamsByType"></param>
        public static GlobalParams ConvertDynamicParamsToGlobalParams(Dictionary<GlobalParamKeyType, dynamic> GlobalParamsByType)
        {
            var globalParams = new GlobalParams();

            foreach (var paramItem in GlobalParamsByType)
            {
                globalParams.AddUpdateValue(paramItem.Key, paramItem.Value);
            }

            return globalParams;
        }
    }
}
