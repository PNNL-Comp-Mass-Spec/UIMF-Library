// --------------------------------------------------------------------------------------------------------------------
// <copyright company="" file="ClassDefinitions.cs">
//   
// </copyright>
// <summary>
//   TODO The global parameters.
// </summary>
// 
// --------------------------------------------------------------------------------------------------------------------
namespace UIMFLibrary
{
	using System;

	/// <summary>
	/// TODO The global parameters.
	/// </summary>
	public class GlobalParameters
	{
		// public DateTime DateStarted;         // 1, Date Experiment was acquired 
		#region Fields

		/// <summary>
		/// TODO The bin width.
		/// </summary>
		public double BinWidth; // 4, Width of TOF bins (in ns)

		/// <summary>
		/// TODO The bins.
		/// </summary>
		public int Bins; // 5, Total number of TOF bins in frame

		/// <summary>
		/// TODO The dataset type.
		/// </summary>
		public string DatasetType; // 9, Type of dataset (HMS/HMSMS/HMS-MSn)

		/// <summary>
		/// TODO The date started.
		/// </summary>
		public string DateStarted;

		/// <summary>
		/// TODO The frame data blob version.
		/// </summary>
		public float FrameDataBlobVersion; // 6, Version of FrameDataBlob in T_Frame

		/// <summary>
		/// TODO The instrument name.
		/// </summary>
		public string InstrumentName; // Name of the system on which data was acquired

		/// <summary>
		/// TODO The num frames.
		/// </summary>
		public int NumFrames; // 2, Number of frames in dataset

		/// <summary>
		/// TODO The prescan_ accumulations.
		/// </summary>
		public int Prescan_Accumulations;

		/// <summary>
		/// TODO The prescan_ continuous.
		/// </summary>
		public bool Prescan_Continuous; // True or False

		/// <summary>
		/// TODO The prescan_ profile.
		/// </summary>
		public string Prescan_Profile; // If continuous is true, set this to NULL;

		/// <summary>
		/// TODO The prescan_ tic threshold.
		/// </summary>
		public int Prescan_TICThreshold;

		/// <summary>
		/// TODO The prescan_ tof pulses.
		/// </summary>
		public int Prescan_TOFPulses; // 10 - 14, Prescan parameter

		/// <summary>
		/// TODO The scan data blob version.
		/// </summary>
		public float ScanDataBlobVersion; // 7, Version of ScanInfoBlob in T_Frame

		/// <summary>
		/// TODO The tof correction time.
		/// </summary>
		public float TOFCorrectionTime;

		/// <summary>
		/// TODO The tof intensity type.
		/// </summary>
		public string TOFIntensityType;
		              // 8, Data type of intensity in each TOF record (ADC is int/TDC is short/FOLDED is float)

		/// <summary>
		/// TODO The time offset.
		/// </summary>
		public int TimeOffset; // 3, Offset from 0. All bin numbers must be offset by this amount

		#endregion
	}

	/// <summary>
	/// TODO The frame parameters.
	/// </summary>
	public class FrameParameters
	{
		#region Fields

		/// <summary>
		/// TODO The accumulations.
		/// </summary>
		public int Accumulations; // 3, Number of collected and summed acquisitions in a frame 

		/// <summary>
		/// TODO The average tof length.
		/// </summary>
		public double AverageTOFLength; // 8, Average time between TOF trigger pulses

		/// <summary>
		/// TODO The calibration done.
		/// </summary>
		public int CalibrationDone = -1; // 47, Set to 1 after a frame has been calibrated

		/// <summary>
		/// TODO The calibration intercept.
		/// </summary>
		public double CalibrationIntercept; // 10, Value of t0  

		/// <summary>
		/// TODO The calibration slope.
		/// </summary>
		public double CalibrationSlope; // 9, Value of k0  

		/// <summary>
		/// TODO The decoded.
		/// </summary>
		public int Decoded = 0; // 48, Set to 1 after a frame has been decoded (added June 27, 2011)

		/// <summary>
		/// TODO The duration.
		/// </summary>
		public double Duration; // 2, Duration of frame, in seconds 

		/// <summary>
		/// TODO The esi voltage.
		/// </summary>
		public double ESIVoltage; // 45

		/// <summary>
		/// TODO The float voltage.
		/// </summary>
		public double FloatVoltage; // 46

		/// <summary>
		/// TODO The fragmentation profile.
		/// </summary>
		public double[] FragmentationProfile; // 40, Voltage profile used in fragmentation

		/// <summary>
		/// TODO The frame num.
		/// </summary>
		public int FrameNum; // 0, Frame number (primary key)     

		/// <summary>
		/// TODO The frame type.
		/// </summary>
		public DataReader.FrameType FrameType;
		                            // 4, Bitmap: 0=MS (Legacy); 1=MS (Regular); 2=MS/MS (Frag); 3=Calibration; 4=Prescan

		/// <summary>
		/// TODO The high pressure funnel pressure.
		/// </summary>
		public double HighPressureFunnelPressure; // 41

		/// <summary>
		/// TODO The imf profile.
		/// </summary>
		public string IMFProfile;
		              // 6, IMFProfile Name; this stores the name of the sequence used to encode the data when acquiring data multiplexed

		/// <summary>
		/// TODO The ion funnel trap pressure.
		/// </summary>
		public double IonFunnelTrapPressure; // 42

		/// <summary>
		/// TODO The mp bit order.
		/// </summary>
		public short MPBitOrder; // 39, Determines original size of bit sequence 

		/// <summary>
		/// TODO The pressure back.
		/// </summary>
		public double PressureBack; // 38, Pressure at back of Drift Tube 

		/// <summary>
		/// TODO The pressure front.
		/// </summary>
		public double PressureFront; // 37, Pressure at front of Drift Tube 

		/// <summary>
		/// TODO The quadrupole pressure.
		/// </summary>
		public double QuadrupolePressure; // 44

		/// <summary>
		/// TODO The rear ion funnel pressure.
		/// </summary>
		public double RearIonFunnelPressure; // 43

		/// <summary>
		/// TODO The scans.
		/// </summary>
		public int Scans; // 5, Number of TOF scans  

		/// <summary>
		/// TODO The start time.
		/// </summary>
		public double StartTime; // 1, Start time of frame, in minutes

		/// <summary>
		/// TODO The tof losses.
		/// </summary>
		public double TOFLosses; // 7, Number of TOF Losses

		/// <summary>
		/// TODO The temperature.
		/// </summary>
		public double Temperature; // 17, Ambient temperature

		/// <summary>
		/// TODO The a 2.
		/// </summary>
		public double a2; // 11, These six parameters below are coefficients for residual mass error correction

		/// <summary>
		/// TODO The b 2.
		/// </summary>
		public double b2; // 12, ResidualMassError=a2t+b2t^3+c2t^5+d2t^7+e2t^9+f2t^11

		/// <summary>
		/// TODO The c 2.
		/// </summary>
		public double c2; // 13

		/// <summary>
		/// TODO The d 2.
		/// </summary>
		public double d2; // 14

		/// <summary>
		/// TODO The e 2.
		/// </summary>
		public double e2; // 15

		/// <summary>
		/// TODO The f 2.
		/// </summary>
		public double f2; // 16

		/// <summary>
		/// TODO The volt cap inlet.
		/// </summary>
		public double voltCapInlet; // 22, Capillary Inlet Voltage

		/// <summary>
		/// TODO The volt cond 1.
		/// </summary>
		public double voltCond1; // 30, Fragmentation Conductance Voltage

		/// <summary>
		/// TODO The volt cond 2.
		/// </summary>
		public double voltCond2; // 32, Fragmentation Conductance Voltage

		/// <summary>
		/// TODO The volt entrance cond lmt.
		/// </summary>
		public double voltEntranceCondLmt; // 25, Cond Limit Voltage

		/// <summary>
		/// TODO The volt entrance hpf in.
		/// </summary>
		public double voltEntranceHPFIn;
		              // 23, HPF In Voltage  (renamed from voltEntranceIFTIn  to voltEntranceHPFIn  in July 2011)

		/// <summary>
		/// TODO The volt entrance hpf out.
		/// </summary>
		public double voltEntranceHPFOut;
		              // 24, HPF Out Voltage (renamed from voltEntranceIFTOut to voltEntranceHPFOut in July 2011)

		/// <summary>
		/// TODO The volt exit cond lmt.
		/// </summary>
		public double voltExitCondLmt; // 36, Cond Limit Voltage

		/// <summary>
		/// TODO The volt exit hpf in.
		/// </summary>
		public double voltExitHPFIn; // 34, HPF In Voltage  (renamed from voltExitIFTIn  to voltExitHPFIn  in July 2011)

		/// <summary>
		/// TODO The volt exit hpf out.
		/// </summary>
		public double voltExitHPFOut; // 35, HPF Out Voltage (renamed from voltExitIFTOut to voltExitHPFOut in July 2011)

		/// <summary>
		/// TODO The volt hv rack 1.
		/// </summary>
		public double voltHVRack1; // 18, Voltage setting in the IMS system

		/// <summary>
		/// TODO The volt hv rack 2.
		/// </summary>
		public double voltHVRack2; // 19, Voltage setting in the IMS system

		/// <summary>
		/// TODO The volt hv rack 3.
		/// </summary>
		public double voltHVRack3; // 20, Voltage setting in the IMS system

		/// <summary>
		/// TODO The volt hv rack 4.
		/// </summary>
		public double voltHVRack4; // 21, Voltage setting in the IMS system

		/// <summary>
		/// TODO The volt ims out.
		/// </summary>
		public double voltIMSOut; // 33, IMS Out Voltage

		/// <summary>
		/// TODO The volt jet dist.
		/// </summary>
		public double voltJetDist; // 28, Jet Disruptor Voltage

		/// <summary>
		/// TODO The volt quad 1.
		/// </summary>
		public double voltQuad1; // 29, Fragmentation Quadrupole Voltage

		/// <summary>
		/// TODO The volt quad 2.
		/// </summary>
		public double voltQuad2; // 31, Fragmentation Quadrupole Voltage

		/// <summary>
		/// TODO The volt trap in.
		/// </summary>
		public double voltTrapIn; // 27, Trap In Voltage

		/// <summary>
		/// TODO The volt trap out.
		/// </summary>
		public double voltTrapOut; // 26, Trap Out Voltage

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
			this.StartTime = System.DateTime.UtcNow.Subtract(dtRunStartTime).TotalMinutes;
		}

		#endregion

		#region Public Properties

		/// <summary>
		/// Included for backwards compatibility
		/// </summary>
		public double voltEntranceIFTIn
		{
			get
			{
				return this.voltEntranceHPFIn;
			}

			set
			{
				this.voltEntranceHPFIn = value;
			}
		}

		/// <summary>
		/// Included for backwards compatibility
		/// </summary>
		public double voltEntranceIFTOut
		{
			get
			{
				return this.voltEntranceHPFOut;
			}

			set
			{
				this.voltEntranceHPFOut = value;
			}
		}

		/// <summary>
		/// Included for backwards compatibility
		/// </summary>
		public double voltExitIFTIn
		{
			get
			{
				return this.voltExitHPFIn;
			}

			set
			{
				this.voltExitHPFIn = value;
			}
		}

		/// <summary>
		/// Included for backwards compatibility
		/// </summary>
		public double voltExitIFTOut
		{
			get
			{
				return this.voltExitHPFOut;
			}

			set
			{
				this.voltExitHPFOut = value;
			}
		}

		#endregion

		#region Public Methods and Operators

		/// <summary>
		/// TODO The copy to.
		/// </summary>
		/// <param name="Target">
		/// TODO The target.
		/// </param>
		public void CopyTo(out FrameParameters Target)
		{
			Target = new FrameParameters();

			Target.FrameNum = this.FrameNum;
			Target.StartTime = this.StartTime;
			Target.Duration = this.Duration;
			Target.Accumulations = this.Accumulations;
			Target.FrameType = this.FrameType;
			Target.Scans = this.Scans;
			Target.IMFProfile = this.IMFProfile;
			Target.TOFLosses = this.TOFLosses;
			Target.AverageTOFLength = this.AverageTOFLength;
			Target.CalibrationSlope = this.CalibrationSlope;
			Target.CalibrationIntercept = this.CalibrationIntercept;
			Target.a2 = this.a2;
			Target.b2 = this.b2;
			Target.c2 = this.c2;
			Target.d2 = this.d2;
			Target.e2 = this.e2;
			Target.f2 = this.f2;
			Target.Temperature = this.Temperature;
			Target.voltHVRack1 = this.voltHVRack1;
			Target.voltHVRack2 = this.voltHVRack2;
			Target.voltHVRack3 = this.voltHVRack3;
			Target.voltHVRack4 = this.voltHVRack4;
			Target.voltCapInlet = this.voltCapInlet;
			Target.voltEntranceHPFIn = this.voltEntranceHPFIn;
			Target.voltEntranceHPFOut = this.voltEntranceHPFOut;
			Target.voltEntranceCondLmt = this.voltEntranceCondLmt;
			Target.voltTrapOut = this.voltTrapOut;
			Target.voltTrapIn = this.voltTrapIn;
			Target.voltJetDist = this.voltJetDist;
			Target.voltQuad1 = this.voltQuad1;
			Target.voltCond1 = this.voltCond1;
			Target.voltQuad2 = this.voltQuad2;
			Target.voltCond2 = this.voltCond2;
			Target.voltIMSOut = this.voltIMSOut;
			Target.voltExitHPFIn = this.voltExitHPFIn;
			Target.voltExitHPFOut = this.voltExitHPFOut;
			Target.voltExitCondLmt = this.voltExitCondLmt;
			Target.PressureFront = this.PressureFront;
			Target.PressureBack = this.PressureBack;
			Target.MPBitOrder = this.MPBitOrder;

			if (this.FragmentationProfile != null)
			{
				Target.FragmentationProfile = new double[this.FragmentationProfile.Length];
				Array.Copy(this.FragmentationProfile, Target.FragmentationProfile, this.FragmentationProfile.Length);
			}

			Target.HighPressureFunnelPressure = this.HighPressureFunnelPressure;
			Target.IonFunnelTrapPressure = this.IonFunnelTrapPressure;
			Target.RearIonFunnelPressure = this.RearIonFunnelPressure;
			Target.QuadrupolePressure = this.QuadrupolePressure;
			Target.ESIVoltage = this.ESIVoltage;
			Target.FloatVoltage = this.FloatVoltage;
			Target.CalibrationDone = this.CalibrationDone;
			Target.Decoded = this.Decoded;
		}

		#endregion
	}

	// /////////////////////////////////////////////////////////////////////
	// Calibrate TOF to m/z according to formula mass = (k * (t-t0))^2
	/// <summary>
	/// TODO The m z_ calibrator.
	/// </summary>
	public class MZ_Calibrator
	{
		#region Fields

		/// <summary>
		/// TODO The k.
		/// </summary>
		private double K;

		/// <summary>
		/// TODO The t 0.
		/// </summary>
		private double T0;

		#endregion

		#region Constructors and Destructors

		/// <summary>
		/// Initializes a new instance of the <see cref="MZ_Calibrator"/> class.
		/// </summary>
		/// <param name="k">
		/// TODO The k.
		/// </param>
		/// <param name="t0">
		/// TODO The t 0.
		/// </param>
		public MZ_Calibrator(double k, double t0)
		{
			this.K = k;
			this.T0 = t0;
		}

		#endregion

		#region Public Properties

		/// <summary>
		/// Gets the description.
		/// </summary>
		public string Description
		{
			get
			{
				return "mz = (k*(t-t0))^2";
			}
		}

		/// <summary>
		/// Gets or sets the k.
		/// </summary>
		public double k
		{
			get
			{
				return this.K;
			}

			set
			{
				this.K = value;
			}
		}

		/// <summary>
		/// Gets or sets the t 0.
		/// </summary>
		public double t0
		{
			get
			{
				return this.T0;
			}

			set
			{
				this.T0 = value;
			}
		}

		#endregion

		#region Public Methods and Operators

		/// <summary>
		/// TODO The m zto tof.
		/// </summary>
		/// <param name="mz">
		/// TODO The mz.
		/// </param>
		/// <returns>
		/// The <see cref="int"/>.
		/// </returns>
		public int MZtoTOF(double mz)
		{
			double r = Math.Sqrt(mz);
			return (int)(((r / this.K) + this.T0) + .5); // .5 for rounding
		}

		/// <summary>
		/// TODO The to fto mz.
		/// </summary>
		/// <param name="TOFValue">
		/// TODO The tof value.
		/// </param>
		/// <returns>
		/// The <see cref="double"/>.
		/// </returns>
		public double TOFtoMZ(double TOFValue)
		{
			double r = this.K * (TOFValue - this.T0);
			return r * r;
		}

		#endregion
	}
}