using System;
using System.Collections.Generic;

namespace UIMFLibrary.UnitTests.Legacy
{
    // ReSharper disable CommentTypo

    // Ignore Spelling: Cond, Electrospray, Frag, hv, Prescan

    // ReSharper restore CommentTypo

    /// <summary>
    /// Frame parameters
    /// </summary>
    [Obsolete("This class has been superseded by the FrameParams class")]
    class FrameParameters
    {
        // Ignore Spelling: Lmt

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
        /// Electrospray voltage.
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
        public UIMFData.FrameType FrameType;

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
        /// <remarks>
        /// This is actually the maximum scan number in the frame,
        /// since a frame might not start at scan 1 and may have missing scans
        /// </remarks>
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
        /// <remarks>
        /// Renamed from voltExitIFTIn to voltExitHPFIn in July 2011
        /// </remarks>
        public double voltExitHPFIn;

        /// <summary>
        /// HPF Out Voltage
        /// </summary>
        /// <remarks>
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

        /// <summary>
        /// Initializes a new instance of the <see cref="FrameParameters"/> class.
        /// This constructor assumes the developer will manually store a value in StartTime
        /// </summary>
        public FrameParameters()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="FrameParameters"/> class.
        /// This constructor auto-populates StartTime using Now minus runStartTime
        /// </summary>
        /// <param name="runStartTime">
        /// </param>
        // ReSharper disable once UnusedMember.Global
        public FrameParameters(DateTime runStartTime)
        {
            StartTime = DateTime.UtcNow.Subtract(runStartTime).TotalMinutes;
        }

        /// <summary>
        /// Included for backwards compatibility
        /// </summary>
        // ReSharper disable once UnusedMember.Global
        public double voltEntranceIFTIn
        {
            get => voltEntranceHPFIn;

            set => voltEntranceHPFIn = value;
        }

        /// <summary>
        /// Included for backwards compatibility
        /// </summary>
        // ReSharper disable once UnusedMember.Global
        public double voltEntranceIFTOut
        {
            get => voltEntranceHPFOut;

            set => voltEntranceHPFOut = value;
        }

        /// <summary>
        /// Included for backwards compatibility
        /// </summary>
        // ReSharper disable once UnusedMember.Global
        public double voltExitIFTIn
        {
            get => voltExitHPFIn;

            set => voltExitHPFIn = value;
        }

        /// <summary>
        /// Included for backwards compatibility
        /// </summary>
        // ReSharper disable once UnusedMember.Global
        public double voltExitIFTOut
        {
            get => voltExitHPFOut;

            set => voltExitHPFOut = value;
        }

        /// <summary>
        /// Copy the frame parameters to a target (deep copy)
        /// </summary>
        /// <param name="Target">
        /// Output: target object
        /// </param>
        // ReSharper disable once UnusedMember.Global
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

        /// <summary>
        /// Convert an array of doubles to an array of bytes
        /// </summary>
        /// <param name="frag">
        /// </param>
        /// <returns>
        /// Byte array
        /// </returns>
        public static byte[] ConvertToBlob(double[] frag)
        {
            if (frag == null)
                frag = Array.Empty<double>();

            // convert the fragmentation profile into an array of bytes
            var length_blob = frag.Length;
            var blob_values = new byte[length_blob * 8];

            Buffer.BlockCopy(frag, 0, blob_values, 0, length_blob * 8);

            return blob_values;
        }

        /// <summary>
        /// Create a frame parameter dictionary using a FrameParameters class instance
        /// </summary>
        /// <param name="frameParameters"></param>
        /// <returns>Frame parameter dictionary</returns>
        internal static Dictionary<FrameParamKeyType, dynamic> ConvertFrameParameters(FrameParameters frameParameters)
        {
            var frameParams = new Dictionary<FrameParamKeyType, dynamic>
            {
                // Start time of frame, in minutes
                {FrameParamKeyType.StartTimeMinutes, frameParameters.StartTime},

                // Duration of frame, in seconds
                {FrameParamKeyType.DurationSeconds, frameParameters.Duration},

                // Number of collected and summed acquisitions in a frame
                {FrameParamKeyType.Accumulations, frameParameters.Accumulations},

                // Bitmap: 0=MS (Legacy); 1=MS (Regular); 2=MS/MS (Frag); 3=Calibration; 4=Prescan
                {FrameParamKeyType.FrameType, (int)frameParameters.FrameType},

                // Set to 1 after a frame has been decoded (added June 27, 2011)
                {FrameParamKeyType.Decoded, frameParameters.Decoded},

                // Set to 1 after a frame has been calibrated
                {FrameParamKeyType.CalibrationDone, frameParameters.CalibrationDone},

                // Number of TOF scans
                {FrameParamKeyType.Scans, frameParameters.Scans},

                // IMFProfile Name; this stores the name of the sequence used to encode the data when acquiring data multiplexed
                {FrameParamKeyType.MultiplexingEncodingSequence, frameParameters.IMFProfile},

                // Original size of bit sequence
                {FrameParamKeyType.MPBitOrder, frameParameters.MPBitOrder},

                // Number of TOF Losses
                {FrameParamKeyType.TOFLosses, frameParameters.TOFLosses},

                // Average time between TOF trigger pulses
                {FrameParamKeyType.AverageTOFLength, frameParameters.AverageTOFLength},

                // Calibration slope, k0
                {FrameParamKeyType.CalibrationSlope, frameParameters.CalibrationSlope},

                // Calibration intercept, t0
                {FrameParamKeyType.CalibrationIntercept, frameParameters.CalibrationIntercept}
            };

            // These six parameters are coefficients for residual mass error correction
            // ResidualMassError = a2*t + b2*t^3 + c2*t^5 + d2*t^7 + e2*t^9 + f2*t^11
            if (Math.Abs(frameParameters.a2) > float.Epsilon ||
                Math.Abs(frameParameters.b2) > float.Epsilon ||
                Math.Abs(frameParameters.c2) > float.Epsilon ||
                Math.Abs(frameParameters.d2) > float.Epsilon ||
                Math.Abs(frameParameters.e2) > float.Epsilon ||
                Math.Abs(frameParameters.f2) > float.Epsilon)
            {
                frameParams.Add(FrameParamKeyType.MassCalibrationCoefficienta2, frameParameters.a2);
                frameParams.Add(FrameParamKeyType.MassCalibrationCoefficientb2, frameParameters.b2);
                frameParams.Add(FrameParamKeyType.MassCalibrationCoefficientc2, frameParameters.c2);
                frameParams.Add(FrameParamKeyType.MassCalibrationCoefficientd2, frameParameters.d2);
                frameParams.Add(FrameParamKeyType.MassCalibrationCoefficiente2, frameParameters.e2);
                frameParams.Add(FrameParamKeyType.MassCalibrationCoefficientf2, frameParameters.f2);
            }

            // Ambient temperature
            frameParams.Add(FrameParamKeyType.AmbientTemperature, frameParameters.Temperature);

            // Voltage settings in the IMS system
            if (Math.Abs(frameParameters.voltHVRack1) > float.Epsilon ||
                Math.Abs(frameParameters.voltHVRack2) > float.Epsilon ||
                Math.Abs(frameParameters.voltHVRack3) > float.Epsilon ||
                Math.Abs(frameParameters.voltHVRack4) > float.Epsilon)
            {
                frameParams.Add(FrameParamKeyType.VoltHVRack1, frameParameters.voltHVRack1);
                frameParams.Add(FrameParamKeyType.VoltHVRack2, frameParameters.voltHVRack2);
                frameParams.Add(FrameParamKeyType.VoltHVRack3, frameParameters.voltHVRack3);
                frameParams.Add(FrameParamKeyType.VoltHVRack4, frameParameters.voltHVRack4);
            }

            // Capillary Inlet Voltage
            // HPF In Voltage
            // HPF Out Voltage
            // Cond Limit Voltage
            if (Math.Abs(frameParameters.voltEntranceHPFIn) > float.Epsilon ||
                Math.Abs(frameParameters.voltEntranceHPFIn) > float.Epsilon ||
                Math.Abs(frameParameters.voltEntranceHPFOut) > float.Epsilon ||
                Math.Abs(frameParameters.voltEntranceCondLmt) > float.Epsilon)
            {
                frameParams.Add(FrameParamKeyType.VoltCapInlet, frameParameters.voltCapInlet);
                frameParams.Add(FrameParamKeyType.VoltEntranceHPFIn, frameParameters.voltEntranceHPFIn);
                frameParams.Add(FrameParamKeyType.VoltEntranceHPFOut, frameParameters.voltEntranceHPFOut);
                frameParams.Add(FrameParamKeyType.VoltEntranceCondLmt, frameParameters.voltEntranceCondLmt);
            }

            // Trap Out Voltage
            // Trap In Voltage
            // Jet Disruptor Voltage
            if (Math.Abs(frameParameters.voltTrapOut) > float.Epsilon ||
                Math.Abs(frameParameters.voltTrapIn) > float.Epsilon ||
                Math.Abs(frameParameters.voltJetDist) > float.Epsilon)
            {
                frameParams.Add(FrameParamKeyType.VoltTrapOut, frameParameters.voltTrapOut);
                frameParams.Add(FrameParamKeyType.VoltTrapIn, frameParameters.voltTrapIn);
                frameParams.Add(FrameParamKeyType.VoltJetDist, frameParameters.voltJetDist);
            }

            // Fragmentation Quadrupole 1 Voltage
            // Fragmentation Conductance 1 Voltage
            if (Math.Abs(frameParameters.voltQuad1) > float.Epsilon ||
                Math.Abs(frameParameters.voltCond1) > float.Epsilon)
            {
                frameParams.Add(FrameParamKeyType.VoltQuad1, frameParameters.voltQuad1);
                frameParams.Add(FrameParamKeyType.VoltCond1, frameParameters.voltCond1);
            }

            // Fragmentation Quadrupole 2 Voltage
            // Fragmentation Conductance 2 Voltage
            if (Math.Abs(frameParameters.voltQuad2) > float.Epsilon ||
                Math.Abs(frameParameters.voltCond2) > float.Epsilon)
            {
                frameParams.Add(FrameParamKeyType.VoltQuad2, frameParameters.voltQuad2);
                frameParams.Add(FrameParamKeyType.VoltCond2, frameParameters.voltCond2);
            }

            // IMS Out Voltage
            // HPF In Voltage
            // HPF Out Voltage
            if (Math.Abs(frameParameters.voltIMSOut) > float.Epsilon ||
                Math.Abs(frameParameters.voltExitHPFIn) > float.Epsilon ||
                Math.Abs(frameParameters.voltExitHPFOut) > float.Epsilon ||
                Math.Abs(frameParameters.voltExitCondLmt) > float.Epsilon)
            {
                frameParams.Add(FrameParamKeyType.VoltIMSOut, frameParameters.voltIMSOut);
                frameParams.Add(FrameParamKeyType.VoltExitHPFIn, frameParameters.voltExitHPFIn);
                frameParams.Add(FrameParamKeyType.VoltExitHPFOut, frameParameters.voltExitHPFOut);
                frameParams.Add(FrameParamKeyType.VoltExitCondLmt, frameParameters.voltExitCondLmt);
            }

            // Pressure at front of Drift Tube
            // Pressure at back of Drift Tube
            if (Math.Abs(frameParameters.PressureFront) > float.Epsilon ||
                Math.Abs(frameParameters.PressureBack) > float.Epsilon)
            {
                frameParams.Add(FrameParamKeyType.PressureFront, frameParameters.PressureFront);
                frameParams.Add(FrameParamKeyType.PressureBack, frameParameters.PressureBack);
            }

            // High pressure funnel pressure
            // Ion funnel trap pressure
            // Rear ion funnel pressure
            // Quadruple pressure
            if (Math.Abs(frameParameters.HighPressureFunnelPressure) > float.Epsilon ||
                Math.Abs(frameParameters.IonFunnelTrapPressure) > float.Epsilon ||
                Math.Abs(frameParameters.RearIonFunnelPressure) > float.Epsilon ||
                Math.Abs(frameParameters.QuadrupolePressure) > float.Epsilon)
            {
                frameParams.Add(FrameParamKeyType.HighPressureFunnelPressure, frameParameters.HighPressureFunnelPressure);
                frameParams.Add(FrameParamKeyType.IonFunnelTrapPressure, frameParameters.IonFunnelTrapPressure);
                frameParams.Add(FrameParamKeyType.RearIonFunnelPressure, frameParameters.RearIonFunnelPressure);
                frameParams.Add(FrameParamKeyType.QuadrupolePressure, frameParameters.QuadrupolePressure);
            }

            // ESI Voltage
            if (Math.Abs(frameParameters.ESIVoltage) > float.Epsilon)
            {
                frameParams.Add(FrameParamKeyType.ESIVoltage, frameParameters.ESIVoltage);
            }

            // Float Voltage
            if (Math.Abs(frameParameters.FloatVoltage) > float.Epsilon)
            {
                frameParams.Add(FrameParamKeyType.FloatVoltage, frameParameters.FloatVoltage);
            }

            // Voltage profile used in fragmentation
            // Legacy parameter, likely never used
            if (frameParameters.FragmentationProfile?.Length > 0)
            {
                // Convert the fragmentation profile (array of doubles) into an array of bytes
                var byteArray = ConvertToBlob(frameParameters.FragmentationProfile);

                // Now convert to a base-64 encoded string
                var base64String = Convert.ToBase64String(byteArray, 0, byteArray.Length);

                // Finally, store in frameParams
                frameParams.Add(FrameParamKeyType.FragmentationProfile, base64String);
            }

            return frameParams;
        }

        // ReSharper disable once UnusedMember.Global

        /// <summary>
        /// Obtain a FrameParameters instance from a FrameParams instance
        /// </summary>
        /// <param name="frameNumber">Frame Number</param>
        /// <param name="frameParameters"><see cref="FrameParams"/> instance</param>
        /// <returns>A new <see cref="FrameParameters"/> instance</returns>
        internal static FrameParameters GetLegacyFrameParameters(int frameNumber, FrameParams frameParameters)
        {
            if (frameParameters == null)
                return new FrameParameters();

            var frameType = frameParameters.FrameType;

            // Populate legacyFrameParams using dictionary frameParams
            var legacyFrameParams = new FrameParameters
            {
                FrameNum = frameNumber,
                StartTime = frameParameters.GetValueDouble(FrameParamKeyType.StartTimeMinutes, 0),
                Duration = frameParameters.GetValueDouble(FrameParamKeyType.DurationSeconds, 0),
                Accumulations = frameParameters.GetValueInt32(FrameParamKeyType.Accumulations, 0),
                FrameType = frameType,
                Decoded = frameParameters.GetValueInt32(FrameParamKeyType.Decoded, 0),
                CalibrationDone = frameParameters.GetValueInt32(FrameParamKeyType.CalibrationDone, 0),
                Scans = frameParameters.Scans,
                IMFProfile = frameParameters.GetValue(FrameParamKeyType.MultiplexingEncodingSequence, string.Empty),
                MPBitOrder = (short)frameParameters.GetValueInt32(FrameParamKeyType.MPBitOrder, 0),
                TOFLosses = frameParameters.GetValueDouble(FrameParamKeyType.TOFLosses, 0),
                AverageTOFLength = frameParameters.GetValueDouble(FrameParamKeyType.AverageTOFLength, 0),
                CalibrationSlope = frameParameters.GetValueDouble(FrameParamKeyType.CalibrationSlope, 0),
                CalibrationIntercept = frameParameters.GetValueDouble(FrameParamKeyType.CalibrationIntercept, 0),
                a2 = frameParameters.GetValueDouble(FrameParamKeyType.MassCalibrationCoefficienta2, 0),
                b2 = frameParameters.GetValueDouble(FrameParamKeyType.MassCalibrationCoefficientb2, 0),
                c2 = frameParameters.GetValueDouble(FrameParamKeyType.MassCalibrationCoefficientc2, 0),
                d2 = frameParameters.GetValueDouble(FrameParamKeyType.MassCalibrationCoefficientd2, 0),
                e2 = frameParameters.GetValueDouble(FrameParamKeyType.MassCalibrationCoefficiente2, 0),
                f2 = frameParameters.GetValueDouble(FrameParamKeyType.MassCalibrationCoefficientf2, 0),
                Temperature = frameParameters.GetValueDouble(FrameParamKeyType.AmbientTemperature, 0),
                voltHVRack1 = frameParameters.GetValueDouble(FrameParamKeyType.VoltHVRack1, 0),
                voltHVRack2 = frameParameters.GetValueDouble(FrameParamKeyType.VoltHVRack2, 0),
                voltHVRack3 = frameParameters.GetValueDouble(FrameParamKeyType.VoltHVRack3, 0),
                voltHVRack4 = frameParameters.GetValueDouble(FrameParamKeyType.VoltHVRack4, 0),
                voltCapInlet = frameParameters.GetValueDouble(FrameParamKeyType.VoltCapInlet, 0),
                voltEntranceHPFIn = frameParameters.GetValueDouble(FrameParamKeyType.VoltEntranceHPFIn, 0),
                voltEntranceHPFOut = frameParameters.GetValueDouble(FrameParamKeyType.VoltEntranceHPFOut, 0),
                voltEntranceCondLmt = frameParameters.GetValueDouble(FrameParamKeyType.VoltEntranceCondLmt, 0),
                voltTrapOut = frameParameters.GetValueDouble(FrameParamKeyType.VoltTrapOut, 0),
                voltTrapIn = frameParameters.GetValueDouble(FrameParamKeyType.VoltTrapIn, 0),
                voltJetDist = frameParameters.GetValueDouble(FrameParamKeyType.VoltJetDist, 0),
                voltQuad1 = frameParameters.GetValueDouble(FrameParamKeyType.VoltQuad1, 0),
                voltCond1 = frameParameters.GetValueDouble(FrameParamKeyType.VoltCond1, 0),
                voltQuad2 = frameParameters.GetValueDouble(FrameParamKeyType.VoltQuad2, 0),
                voltCond2 = frameParameters.GetValueDouble(FrameParamKeyType.VoltCond2, 0),
                voltIMSOut = frameParameters.GetValueDouble(FrameParamKeyType.VoltIMSOut, 0),
                voltExitHPFIn = frameParameters.GetValueDouble(FrameParamKeyType.VoltExitHPFIn, 0),
                voltExitHPFOut = frameParameters.GetValueDouble(FrameParamKeyType.VoltExitHPFOut, 0),
                voltExitCondLmt = frameParameters.GetValueDouble(FrameParamKeyType.VoltExitCondLmt, 0),
                PressureFront = frameParameters.GetValueDouble(FrameParamKeyType.PressureFront, 0),
                PressureBack = frameParameters.GetValueDouble(FrameParamKeyType.PressureBack, 0),
                HighPressureFunnelPressure = frameParameters.GetValueDouble(FrameParamKeyType.HighPressureFunnelPressure, 0),
                IonFunnelTrapPressure = frameParameters.GetValueDouble(FrameParamKeyType.IonFunnelTrapPressure, 0),
                RearIonFunnelPressure = frameParameters.GetValueDouble(FrameParamKeyType.RearIonFunnelPressure, 0),
                QuadrupolePressure = frameParameters.GetValueDouble(FrameParamKeyType.QuadrupolePressure, 0),
                ESIVoltage = frameParameters.GetValueDouble(FrameParamKeyType.ESIVoltage, 0),
                FloatVoltage = frameParameters.GetValueDouble(FrameParamKeyType.FloatVoltage, 0)
            };

            var fragmentationProfile = frameParameters.GetValue(FrameParamKeyType.FragmentationProfile, string.Empty);

            if (string.IsNullOrEmpty(fragmentationProfile))
            {
                legacyFrameParams.FragmentationProfile = Array.Empty<double>();
            }
            else
            {
                // The fragmentation profile was stored as an array of bytes, encoded as base 64

                // Convert back to bytes
                var byteArray = Convert.FromBase64String(fragmentationProfile);

                // Now convert from array of bytes to array of doubles
                legacyFrameParams.FragmentationProfile = FrameParamUtilities.ConvertByteArrayToFragmentationSequence(byteArray);
            }

            return legacyFrameParams;
        }
    }
}
