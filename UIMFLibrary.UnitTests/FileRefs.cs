namespace UIMFLibrary.UnitTests
{
    /// <summary>
    /// File references
    /// </summary>
    public static class FileRefs
    {
        // Ignore Spelling: demultiplexed, demultiplexing, uimf

        // ReSharper disable once IdentifierTypo
        private const string DECONTOOLS_SHARE_PATH = @"\\proto-2\UnitTest_Files\DeconTools_TestFiles\UIMF\";

        public const string SHARE_PATH = @"\\proto-2\unitTest_Files\UIMF_Library\";

        #region Static Fields

        /// <summary>
        /// UIMF file with encoded spectra (data before demultiplexing)
        /// </summary>
        /// <remarks>\\proto-2\UnitTest_Files\DeconTools_TestFiles\UIMF\Sarc_MS2_90_6Apr11_Cheetah_11-02-19_encoded.uimf</remarks>
        public static string EncodedUIMF = DECONTOOLS_SHARE_PATH + "Sarc_MS2_90_6Apr11_Cheetah_11-02-19_encoded.uimf";

        /// <summary>
        /// UIMF file with MS/MS data
        /// </summary>
        /// <remarks>\\proto-2\UnitTest_Files\DeconTools_TestFiles\UIMF\MSMS_Testing\PepMix_MSMS_4msSA.uimf</remarks>
        public static string MSMSData1 = SHARE_PATH + @"MSMS_Testing\PepMix_MSMS_4msSA.uimf";

        /// <summary>
        /// Demultiplexed UIMF file (legacy tables)
        /// </summary>
        /// <remarks>\\proto-2\UnitTest_Files\DeconTools_TestFiles\UIMF\Sarc_MS2_90_6Apr11_Cheetah_11-02-19.uimf</remarks>
        public static string LegacyDemultiplexedFile1 = SHARE_PATH + "Sarc_MS2_90_6Apr11_Cheetah_11-02-19.uimf";

        /// <summary>
        /// Standard UIMF file (legacy tables)
        /// </summary>
        /// <remarks>\\proto-2\UnitTest_Files\DeconTools_TestFiles\UIMF\Sarc_MS_90_21Aug10_Cheetah_10-08-02_0000.uimf</remarks>
        public static string LegacyFile1 = SHARE_PATH + "Sarc_MS_90_21Aug10_Cheetah_10-08-02_0000.uimf";

        /// <summary>
        /// Same data as LegacyFile1 but with the new Global_Params and Frame_Params tables
        /// </summary>
        /// <remarks>\\proto-2\UnitTest_Files\DeconTools_TestFiles\UIMF\Sarc_MS_90_21Aug10_Cheetah_10-08-02_0000_v3.uimf</remarks>
        public static string StandardFile1 = SHARE_PATH + "Sarc_MS_90_21Aug10_Cheetah_10-08-02_0000_v3.uimf";

        /// <summary>
        /// File for bin-centric tests
        /// </summary>
        public static string BinCentricTest1 = SHARE_PATH + @"BinCentric\Sarc_MS_75_24Aug10_Cheetah_10-08-02_0000_Excerpt.uimf";

        /// <summary>
        /// MS/MS file for bin-centric tests
        /// </summary>
        public static string BinCentricTest2 = SHARE_PATH + @"MSMS_Testing\PepMix_MSMS_4msSA.UIMF";

        /// <summary>
        /// File for writing test data
        /// </summary>
        public static string WriterTest10Frames = "WriterTest_10Frames.uimf";

        /// <summary>
        /// UIMF file with 25 frames
        /// </summary>
        /// <remarks>Tracked by .git</remarks>
        public static string LocalUimfFile25Frames = @"Test_Data\QC_Shew_16_01_Run-3_25Jul16_Oak_16-03-14_Excerpt.uimf";

        /// <summary>
        /// UIMF file with 10 frames using legacy tables
        /// </summary>
        /// <remarks>Tracked by .git</remarks>
        public static string LocalUimfFileLegacyTables = @"Test_Data\Sarc_MS2_90_6Apr11_Cheetah_11-02-19_Excerpt.uimf";

        #endregion
    }
}