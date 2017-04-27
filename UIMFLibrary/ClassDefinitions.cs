// --------------------------------------------------------------------------------------------------------------------
// <summary>
//   Global parameters.
// </summary>
//
// --------------------------------------------------------------------------------------------------------------------
namespace UIMFLibrary
{
    using System;

    /// <summary>
    /// The global parameters.
    /// </summary>
    [Obsolete("This class has been superseded by the GlobalParams class")]
    public class GlobalParameters
    {
        #region Fields

        /// <summary>
        ///Width of TOF bins (in ns)
        /// </summary>
        public double BinWidth;

        /// <summary>
        /// Total number of TOF bins in frame
        /// </summary>
        public int Bins;

        /// <summary>
        /// Type of dataset (HMS/HMSMS/HMS-MSn)
        /// </summary>
        public string DatasetType;

        /// <summary>
        /// Date started.
        /// </summary>
        /// <remarks>
        /// Format has traditionally been M/d/yyyy hh:mm:ss tt
        /// For example, 6/4/2014 12:56:44 PM</remarks>
        public string DateStarted;

        /// <summary>
        /// Version of FrameDataBlob in T_Frame
        /// </summary>
        /// <remarks>Obsolete / never used</remarks>
        public float FrameDataBlobVersion;

        /// <summary>
        /// Instrument name.
        /// </summary>
        public string InstrumentName;

        /// <summary>
        /// Number of frames in dataset
        /// </summary>
        public int NumFrames;

        /// <summary>
        /// Number of prescan accumulations
        /// </summary>
        public int Prescan_Accumulations;

        /// <summary>
        /// Prescan Continuous flag
        /// </summary>
        public bool Prescan_Continuous;

        /// <summary>
        /// Prescan profile.
        /// </summary>
        /// <remarks>
        /// If continuous is true, set this to NULL;
        /// </remarks>
        public string Prescan_Profile;

        /// <summary>
        /// Prescan TIC threshold
        /// </summary>
        public int Prescan_TICThreshold;

        /// <summary>
        /// Prescan TOF pulses
        /// </summary>
        public int Prescan_TOFPulses;

        /// <summary>
        /// Version of ScanInfoBlob in T_Frame
        /// </summary>
        /// <remarks>Obsolete / never used</remarks>
        public float ScanDataBlobVersion;

        /// <summary>
        /// TOF correction time.
        /// </summary>
        public float TOFCorrectionTime;

        /// <summary>
        /// Data type of intensity in each TOF record (ADC is int, TDC is short, FOLDED is float)
        /// </summary>
        public string TOFIntensityType;

        /// <summary>
        /// Time offset from 0. All bin numbers must be offset by this amount
        /// </summary>
        public int TimeOffset;

        #endregion
    }

    /// <summary>
    /// The frame parameters.
    /// </summary>
    [Obsolete("This class has been superseded by the FrameParams class")]
    public class FrameParameters
    {
        #region Fields

        /// <summary>
        /// Number of collected and summed acquisitions in a frame
        /// </summary>
        public int Accumulations;

        /// <summary>
        /// Average TOF length, in nanoseconds
        /// </summary>
        /// <remarks>
        /// Average time between TOF trigger pulses
        /// </remarks>
        public double AverageTOFLength;

        /// <summary>
        /// Tracks whether frame has been calibrated
        /// </summary>
        /// <remarks>
        /// Set to 1 after a frame has been calibrated
        /// </remarks>
        public int CalibrationDone = -1;

        /// <summary>
        /// Calibration intercept, t0
        /// </summary>
        public double CalibrationIntercept;

        /// <summary>
        /// Calibration slope, k0
        /// </summary>
        public double CalibrationSlope;

        /// <summary>
        /// Tracks whether frame has been decoded
        /// </summary>
        /// <remarks>
        /// Set to 1 after a frame has been decoded (added June 27, 2011)
        /// </remarks>
        public int Decoded;

        /// <summary>
        /// Frame duration, in seconds
        /// </summary>
        public double Duration;

        /// <summary>
        /// Electropsray voltage.
        /// </summary>
        public double ESIVoltage;

        /// <summary>
        /// Float voltage.
        /// </summary>
        public double FloatVoltage;

        /// <summary>
        /// Voltage profile used in fragmentation
        /// </summary>
        public double[] FragmentationProfile;

        /// <summary>
        /// Frame number
        /// </summary>
        public int FrameNum;

        /// <summary>
        /// Frame type
        /// </summary>
        /// <remarks>
        /// Bitmap: 0=MS (Legacy); 1=MS (Regular); 2=MS/MS (Frag); 3=Calibration; 4=Prescan
        /// </remarks>
        public DataReader.FrameType FrameType;

        /// <summary>
        /// High pressure funnel pressure.
        /// </summary>
        public double HighPressureFunnelPressure;

        /// <summary>
        /// IMFProfile Name
        /// </summary>
        /// <remarks>
        /// Stores the name of the sequence used to encode the data when acquiring data multiplexed
        /// </remarks>
        public string IMFProfile;

        /// <summary>
        /// Ion funnel trap pressure.
        /// </summary>
        public double IonFunnelTrapPressure;

        /// <summary>
        /// MP bit order
        /// </summary>
        /// <remarks>
        /// Determines original size of bit sequence
        /// </remarks>
        public short MPBitOrder;

        /// <summary>
        /// Pressure at back of Drift Tube
        /// </summary>
        public double PressureBack;

        /// <summary>
        ///  Pressure at front of Drift Tube
        /// </summary>
        public double PressureFront;

        /// <summary>
        /// Quadrupole pressure.
        /// </summary>
        public double QuadrupolePressure;

        /// <summary>
        /// Rear ion funnel pressure.
        /// </summary>
        public double RearIonFunnelPressure;

        /// <summary>
        /// Number of TOF scans in a frame
        /// </summary>
        public int Scans;

        /// <summary>
        /// Start time of frame, in minutes
        /// </summary>
        public double StartTime;

        /// <summary>
        /// Number of TOF Losses (lost/skipped scans due to I/O problems)
        /// </summary>
        public double TOFLosses;

        /// <summary>
        /// Ambient temperature
        /// </summary>
        public double Temperature;

        /// <summary>
        /// a2 parameter for residual mass error correction
        /// </summary>
        /// <remarks>
        /// ResidualMassError = a2*t + b2*t^3 + c2*t^5 + d2*t^7 + e2*t^9 + f2*t^11
        /// </remarks>
        public double a2;

        /// <summary>
        /// b2 parameter for residual mass error correction
        /// </summary>
        public double b2;

        /// <summary>
        /// c2 parameter for residual mass error correction
        /// </summary>
        public double c2;

        /// <summary>
        /// d2 parameter for residual mass error correction
        /// </summary>
        public double d2;

        /// <summary>
        /// e2 parameter for residual mass error correction
        /// </summary>
        public double e2;

        /// <summary>
        /// f2 parameter for residual mass error correction
        /// </summary>
        /// <remarks>
        /// ResidualMassError = a2t + b2t^3 + c2t^5 + d2t^7 + e2t^9 + f2t^11
        /// </remarks>
        public double f2;

        /// <summary>
        /// Capillary Inlet Voltage
        /// </summary>
        public double voltCapInlet;

        /// <summary>
        /// Fragmentation Conductance Voltage
        /// </summary>
        public double voltCond1;

        /// <summary>
        /// Fragmentation Conductance Voltage
        /// </summary>
        public double voltCond2;

        /// <summary>
        /// Entrance Cond Limit Voltage
        /// </summary>
        public double voltEntranceCondLmt;

        /// <summary>
        /// HPF In Voltage
        /// </summary>
        /// <remarks>
        /// Renamed from voltEntranceIFTIn to voltEntranceHPFIn in July 2011
        /// </remarks>
        public double voltEntranceHPFIn;

        /// <summary>
        /// HPF Out Voltage
        /// </summary>
        /// <remarks>
        /// Renamed from voltEntranceIFTOut to voltEntranceHPFOut in July 2011
        /// </remarks>
        public double voltEntranceHPFOut;

        /// <summary>
        /// Exit Cond Limit Voltage
        /// </summary>
        public double voltExitCondLmt;

        /// <summary>
        /// HPF In Voltage
        /// </summary>
        /// /// <remarks>
        /// Renamed from voltExitIFTIn to voltExitHPFIn in July 2011
        /// </remarks>
        public double voltExitHPFIn;

        /// <summary>
        /// HPF Out Voltage
        /// </summary>
        /// /// <remarks>
        /// Renamed from voltExitIFTOut to voltExitHPFOut in July 2011
        /// </remarks>
        public double voltExitHPFOut;

        /// <summary>
        /// Volt hv rack 1.
        /// </summary>
        public double voltHVRack1;

        /// <summary>
        /// Volt hv rack 2.
        /// </summary>
        public double voltHVRack2;

        /// <summary>
        /// Volt hv rack 3.
        /// </summary>
        public double voltHVRack3;

        /// <summary>
        /// Volt hv rack 4.
        /// </summary>
        public double voltHVRack4;

        /// <summary>
        /// IMS Out Voltage
        /// </summary>
        public double voltIMSOut;

        /// <summary>
        /// Jet Disruptor Voltage
        /// </summary>
        public double voltJetDist;

        /// <summary>
        /// Fragmentation Quadrupole Voltage 1
        /// </summary>
        public double voltQuad1;

        /// <summary>
        /// Fragmentation Quadrupole Voltage 2
        /// </summary>
        public double voltQuad2;

        /// <summary>
        /// Trap In Voltage
        /// </summary>
        public double voltTrapIn;

        /// <summary>
        /// Trap Out Voltage
        /// </summary>
        public double voltTrapOut;

        #endregion

        #region Constructors and Destructors

        /// <summary>
        /// Initializes a new instance of the <see cref="FrameParameters"/> class.
        /// This constructor assumes the developer will manually store a value in StartTime
        /// </summary>
        public FrameParameters()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="FrameParameters"/> class.
        /// This constructor auto-populates StartTime using Now minutes dtRunStartTime using the correct format
        /// </summary>
        /// <param name="dtRunStartTime">
        /// </param>
        public FrameParameters(DateTime dtRunStartTime)
        {
            StartTime = DateTime.UtcNow.Subtract(dtRunStartTime).TotalMinutes;
        }

        #endregion

        #region Public Properties

        /// <summary>
        /// Included for backwards compatibility
        /// </summary>
        public double voltEntranceIFTIn
        {
            get => voltEntranceHPFIn;

            set => voltEntranceHPFIn = value;
        }

        /// <summary>
        /// Included for backwards compatibility
        /// </summary>
        public double voltEntranceIFTOut
        {
            get => voltEntranceHPFOut;

            set => voltEntranceHPFOut = value;
        }

        /// <summary>
        /// Included for backwards compatibility
        /// </summary>
        public double voltExitIFTIn
        {
            get => voltExitHPFIn;

            set => voltExitHPFIn = value;
        }

        /// <summary>
        /// Included for backwards compatibility
        /// </summary>
        public double voltExitIFTOut
        {
            get => voltExitHPFOut;

            set => voltExitHPFOut = value;
        }

        #endregion

        #region Public Methods and Operators

        /// <summary>
        /// Copy the frame parameters to a target (deep copy)
        /// </summary>
        /// <param name="Target">
        /// Output: target object
        /// </param>
        public void CopyTo(out FrameParameters Target)
        {
            Target = new FrameParameters
            {
                FrameNum = FrameNum,
                StartTime = StartTime,
                Duration = Duration,
                Accumulations = Accumulations,
                FrameType = FrameType,
                Scans = Scans,
                IMFProfile = IMFProfile,
                TOFLosses = TOFLosses,
                AverageTOFLength = AverageTOFLength,
                CalibrationSlope = CalibrationSlope,
                CalibrationIntercept = CalibrationIntercept,
                a2 = a2,
                b2 = b2,
                c2 = c2,
                d2 = d2,
                e2 = e2,
                f2 = f2,
                Temperature = Temperature,
                voltHVRack1 = voltHVRack1,
                voltHVRack2 = voltHVRack2,
                voltHVRack3 = voltHVRack3,
                voltHVRack4 = voltHVRack4,
                voltCapInlet = voltCapInlet,
                voltEntranceHPFIn = voltEntranceHPFIn,
                voltEntranceHPFOut = voltEntranceHPFOut,
                voltEntranceCondLmt = voltEntranceCondLmt,
                voltTrapOut = voltTrapOut,
                voltTrapIn = voltTrapIn,
                voltJetDist = voltJetDist,
                voltQuad1 = voltQuad1,
                voltCond1 = voltCond1,
                voltQuad2 = voltQuad2,
                voltCond2 = voltCond2,
                voltIMSOut = voltIMSOut,
                voltExitHPFIn = voltExitHPFIn,
                voltExitHPFOut = voltExitHPFOut,
                voltExitCondLmt = voltExitCondLmt,
                PressureFront = PressureFront,
                PressureBack = PressureBack,
                MPBitOrder = MPBitOrder
            };

            if (FragmentationProfile != null)
            {
                Target.FragmentationProfile = new double[FragmentationProfile.Length];
                Array.Copy(FragmentationProfile, Target.FragmentationProfile, FragmentationProfile.Length);
            }

            Target.HighPressureFunnelPressure = HighPressureFunnelPressure;
            Target.IonFunnelTrapPressure = IonFunnelTrapPressure;
            Target.RearIonFunnelPressure = RearIonFunnelPressure;
            Target.QuadrupolePressure = QuadrupolePressure;
            Target.ESIVoltage = ESIVoltage;
            Target.FloatVoltage = FloatVoltage;
            Target.CalibrationDone = CalibrationDone;
            Target.Decoded = Decoded;
        }

        #endregion
    }

}