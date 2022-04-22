using System;
using System.Collections.Generic;

namespace UIMFLibrary
{
    /// <summary>
    /// Utility functions for working with frame parameters
    /// </summary>
    public static class FrameParamUtilities
    {
        // Ignore Spelling: Cond, Frag, hv, Prescan

        #region Member variables

        private static readonly Dictionary<Type, dynamic> mDefaultValuesByType = new Dictionary<Type, dynamic>();

        private static readonly Dictionary<FrameParamKeyType, Type> mFrameParamKeyTypes = new Dictionary<FrameParamKeyType, Type>();

        #endregion

        /// <summary>
        /// Convert the array of bytes defining a fragmentation sequence to an array of doubles
        /// </summary>
        /// <param name="blob">
        /// </param>
        /// <returns>
        /// Array of doubles
        /// </returns>
        public static double[] ConvertByteArrayToFragmentationSequence(byte[] blob)
        {
            var frag = new double[blob.Length / 8];

            for (var i = 0; i < frag.Length; i++)
            {
                frag[i] = BitConverter.ToDouble(blob, i * 8);
            }

            return frag;
        }

        /// <summary>
        /// Convert a dynamic value to a string
        /// </summary>
        /// <param name="value"></param>
        /// <param name="convertedValue"></param>
        public static bool ConvertDynamicToDouble(dynamic value, out double convertedValue)
        {
            if (value is double || value is float || value is int || value is short || value is byte)
            {
                convertedValue = value;
                return true;
            }

            if (value is string)
            {
                if (double.TryParse(value, out double result))
                {
                    convertedValue = result;
                    return true;
                }
            }
            else
            {
                if (double.TryParse(value.ToString(), out double result))
                {
                    convertedValue = result;
                    return true;
                }
            }

            convertedValue = 0;
            return false;
        }

        /// <summary>
        /// Convert a dynamic value to an integer
        /// </summary>
        /// <param name="value"></param>
        /// <param name="convertedValue"></param>
        public static bool ConvertDynamicToInt32(dynamic value, out int convertedValue)
        {
            if (value is int || value is short || value is byte)
            {
                convertedValue = value;
                return true;
            }

            if (value is string)
            {
                if (int.TryParse(value, out int result))
                {
                    convertedValue = result;
                    return true;
                }
            }
            else
            {
                if (int.TryParse(value.ToString(), out int result))
                {
                    convertedValue = result;
                    return true;
                }
            }

            convertedValue = 0;
            return false;
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
        /// Convert the string value to a dynamic variable of the given type
        /// </summary>
        /// <remarks>
        /// Supports byte, short, int, float, double, and DateTime
        /// All other types will continue to be strings
        /// </remarks>
        /// <param name="targetType"></param>
        /// <param name="value"></param>
        /// <param name="returnNullOnError">When true, return null if the conversion fails; when false, return the value as a string</param>
        public static dynamic ConvertStringToDynamic(Type targetType, string value, bool returnNullOnError = true)
        {
            try
            {
                if (value == null)
                    value = string.Empty;

                if (targetType == typeof(byte))
                {
                    if (value.EndsWith(".0"))
                        value = value.Substring(0, value.Length - 2);

                    if (byte.TryParse(value, out var parsed))
                        return parsed;

                    if (!returnNullOnError)
                        Console.WriteLine("Warning: cannot convert {0} to a {1}; will store as a string", value, targetType);
                }

                if (targetType == typeof(short))
                {
                    if (value.EndsWith(".0"))
                        value = value.Substring(0, value.Length - 2);

                    if (short.TryParse(value, out var parsed))
                        return parsed;

                    if (!returnNullOnError)
                        Console.WriteLine("Warning: cannot convert {0} to a {1}; will store as a string", value, targetType);
                }

                if (targetType == typeof(int))
                {
                    if (value.EndsWith(".0"))
                        value = value.Substring(0, value.Length - 2);

                    if (int.TryParse(value, out var parsed))
                        return parsed;

                    if (!returnNullOnError)
                        Console.WriteLine("Warning: cannot convert {0} to a {1}; will store as a string", value, targetType);
                }

                if (targetType == typeof(float))
                {
                    // Interpret null or empty string as NaN
                    if (string.IsNullOrEmpty(value))
                        return float.NaN;

                    if (value.Equals("Inf", StringComparison.OrdinalIgnoreCase))
                        return float.MaxValue;

                    if (value.Equals("-Inf", StringComparison.OrdinalIgnoreCase))
                        return float.MinValue;

                    if (float.TryParse(value, out var parsed))
                        return parsed;

                    if (!returnNullOnError)
                        Console.WriteLine("Warning: cannot convert {0} to a {1}; will store as a string", value, targetType);
                }

                if (targetType == typeof(double))
                {
                    // Interpret null or empty string as NaN
                    if (string.IsNullOrEmpty(value))
                        return double.NaN;

                    if (value.Equals("Inf", StringComparison.OrdinalIgnoreCase))
                        return double.MaxValue;

                    if (value.Equals("-Inf", StringComparison.OrdinalIgnoreCase))
                        return double.MinValue;

                    if (double.TryParse(value, out var parsed))
                        return parsed;

                    if (!returnNullOnError)
                        Console.WriteLine("Warning: cannot convert {0} to a {1}; will store as a string", value, targetType);
                }

                if (targetType == typeof(DateTime))
                {
                    if (DateTime.TryParse(value, out var parsed))
                        return parsed;

                    if (!returnNullOnError)
                        Console.WriteLine("Warning: cannot convert {0} to a {1}; will store as a string", value, targetType);
                }

                if (targetType == typeof(string))
                {
                    return value;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Cannot convert {0} to a {1}: {2}", value, targetType, ex);
            }

            return returnNullOnError ? null : value;
        }

        /// <summary>
        /// Get the default value for the data type associated with the given frame param key
        /// </summary>
        /// <param name="paramType"></param>
        public static dynamic GetDefaultValueByType(FrameParamKeyType paramType)
        {
            var dataType = GetFrameParamKeyDataType(paramType);
            return GetDefaultValueByType(dataType);
        }

        /// <summary>
        /// Get the default value for the given data type
        /// </summary>
        /// <remarks>This method is used by this class and by GlobalParamUtilities</remarks>
        /// <param name="dataType"></param>
        public static dynamic GetDefaultValueByType(Type dataType)
        {
            if (!dataType.IsValueType)
            {
                return dataType == typeof(string) ? string.Empty : null;
            }

            if (mDefaultValuesByType.TryGetValue(dataType, out var defaultValue))
                return defaultValue;

            var defaultForType = Activator.CreateInstance(dataType);
            mDefaultValuesByType.Add(dataType, defaultForType);

            return defaultForType;
        }

        /// <summary>
        /// Get the system data type associated with a given frame parameter key
        /// </summary>
        /// <param name="paramType"></param>
        public static Type GetFrameParamKeyDataType(FrameParamKeyType paramType)
        {
            if (mFrameParamKeyTypes.Count == 0)
            {
                var keyTypes = new Dictionary<FrameParamKeyType, Type>
                {
                    {FrameParamKeyType.StartTimeMinutes, typeof(double)},
                    {FrameParamKeyType.DurationSeconds, typeof(double)},
                    {FrameParamKeyType.Accumulations, typeof(int)},
                    {FrameParamKeyType.FrameType, typeof(int)},
                    {FrameParamKeyType.Decoded, typeof(int)},
                    {FrameParamKeyType.CalibrationDone, typeof(int)},
                    {FrameParamKeyType.Scans, typeof(int)},
                    {FrameParamKeyType.MultiplexingEncodingSequence, typeof(string)},
                    {FrameParamKeyType.MPBitOrder, typeof(int)},
                    {FrameParamKeyType.TOFLosses, typeof(int)},
                    {FrameParamKeyType.AverageTOFLength, typeof(double)},
                    {FrameParamKeyType.CalibrationSlope, typeof(double)},
                    {FrameParamKeyType.CalibrationIntercept, typeof(double)},
                    {FrameParamKeyType.MassCalibrationCoefficienta2, typeof(double)},
                    {FrameParamKeyType.MassCalibrationCoefficientb2, typeof(double)},
                    {FrameParamKeyType.MassCalibrationCoefficientc2, typeof(double)},
                    {FrameParamKeyType.MassCalibrationCoefficientd2, typeof(double)},
                    {FrameParamKeyType.MassCalibrationCoefficiente2, typeof(double)},
                    {FrameParamKeyType.MassCalibrationCoefficientf2, typeof(double)},
                    {FrameParamKeyType.AmbientTemperature, typeof(float)},
                    {FrameParamKeyType.DriftTubeTemperature, typeof(float)},
                    {FrameParamKeyType.VoltHVRack1, typeof(float)},
                    {FrameParamKeyType.VoltHVRack2, typeof(float)},
                    {FrameParamKeyType.VoltHVRack3, typeof(float)},
                    {FrameParamKeyType.VoltHVRack4, typeof(float)},
                    {FrameParamKeyType.VoltCapInlet, typeof(float)},
                    {FrameParamKeyType.VoltEntranceHPFIn, typeof(float)},
                    {FrameParamKeyType.VoltEntranceHPFOut, typeof(float)},
                    {FrameParamKeyType.VoltEntranceCondLmt, typeof(float)},
                    {FrameParamKeyType.VoltTrapOut, typeof(float)},
                    {FrameParamKeyType.VoltTrapIn, typeof(float)},
                    {FrameParamKeyType.VoltJetDist, typeof(float)},
                    {FrameParamKeyType.VoltQuad1, typeof(float)},
                    {FrameParamKeyType.VoltCond1, typeof(float)},
                    {FrameParamKeyType.VoltQuad2, typeof(float)},
                    {FrameParamKeyType.VoltCond2, typeof(float)},
                    {FrameParamKeyType.VoltIMSOut, typeof(float)},
                    {FrameParamKeyType.VoltExitHPFIn, typeof(float)},
                    {FrameParamKeyType.VoltExitHPFOut, typeof(float)},
                    {FrameParamKeyType.VoltExitCondLmt, typeof(float)},
                    {FrameParamKeyType.PressureFront, typeof(float)},
                    {FrameParamKeyType.PressureBack, typeof(float)},
                    {FrameParamKeyType.HighPressureFunnelPressure, typeof(float)},
                    {FrameParamKeyType.IonFunnelTrapPressure, typeof(float)},
                    {FrameParamKeyType.RearIonFunnelPressure, typeof(float)},
                    {FrameParamKeyType.QuadrupolePressure, typeof(float)},
                    {FrameParamKeyType.ESIVoltage, typeof(float)},
                    {FrameParamKeyType.FloatVoltage, typeof(float)},
                    {FrameParamKeyType.FragmentationProfile, typeof(string)},
                    {FrameParamKeyType.ScanNumFirst, typeof(int)},
                    {FrameParamKeyType.ScanNumLast, typeof(int)},
                    {FrameParamKeyType.PressureUnits, typeof(PressureUnits)}
                };

                foreach (var item in keyTypes)
                {
                    mFrameParamKeyTypes.Add(item.Key, item.Value);
                }
            }

            if (mFrameParamKeyTypes.TryGetValue(paramType, out var dataType))
                return dataType;

            throw new ArgumentOutOfRangeException(nameof(paramType), "Unrecognized frame param enum for paramType: " + (int)paramType);
        }

        /// <summary>
        /// Resolve FrameParam Key Type using the parameter id integer value
        /// </summary>
        /// <param name="paramID"></param>
        /// <returns>Specific FrameParamKeyType enum, or FrameParamKeyType.Unknown</returns>
        public static FrameParamKeyType GetParamTypeByID(int paramID)
        {
            if (Enum.IsDefined(typeof(FrameParamKeyType), paramID))
                return (FrameParamKeyType)paramID;

            return FrameParamKeyType.Unknown;
        }

        /// <summary>
        /// Resolve FrameParam Key Type using the parameter name
        /// </summary>
        /// <param name="paramName"></param>
        /// <returns>Specific FrameParamKeyType enum, or FrameParamKeyType.Unknown</returns>
        public static FrameParamKeyType GetParamTypeByName(string paramName)
        {
            // On the first iteration of the while loop, we require a case-sensitive match
            // If no match is found, then on the second iteration we use a case-insensitive match
            for (var iteration = 0; iteration < 2; iteration++)
            {
                try
                {
                    // Note that this conversion works for both the names in the FrameParamKeyType enum and for the integer values
                    // See MSDN's "Enum.Parse Method" page at http://msdn.microsoft.com/en-us/library/essfb559.aspx
                    var ignoreCase = iteration > 0;
                    var paramType = (FrameParamKeyType)Enum.Parse(typeof(FrameParamKeyType), paramName, ignoreCase);
                    if (Enum.IsDefined(typeof(FrameParamKeyType), paramType) || paramType.ToString().Contains(","))
                    {
                        // Match found
                        return paramType;
                    }

                    // No match

                }
                catch (ArgumentException)
                {
                    // No match
                }
            }

            // Auto-conversion failed
            // Look for a synonym
            switch (paramName)
            {
                case "StartTime":
                    return FrameParamKeyType.StartTimeMinutes;

                case "Duration":
                    return FrameParamKeyType.DurationSeconds;

                case "IMFProfile":
                    return FrameParamKeyType.MultiplexingEncodingSequence;

                case "a2":
                    return FrameParamKeyType.MassCalibrationCoefficienta2;
                case "b2":
                    return FrameParamKeyType.MassCalibrationCoefficientb2;
                case "c2":
                    return FrameParamKeyType.MassCalibrationCoefficientc2;
                case "d2":
                    return FrameParamKeyType.MassCalibrationCoefficientd2;
                case "e2":
                    return FrameParamKeyType.MassCalibrationCoefficiente2;
                case "f2":
                    return FrameParamKeyType.MassCalibrationCoefficientf2;

                case "Temperature":
                    return FrameParamKeyType.AmbientTemperature;

                case "DriftTubeTemperature":
                    return FrameParamKeyType.DriftTubeTemperature;

                case "voltEntranceIFTIn":
                    return FrameParamKeyType.VoltEntranceHPFIn;
                case "voltEntranceIFTOut":
                    return FrameParamKeyType.VoltEntranceHPFOut;

                case "voltExitIFTIn":
                    return FrameParamKeyType.VoltExitHPFIn;
                case "voltExitIFTOut":
                    return FrameParamKeyType.VoltExitHPFOut;

                default:
                    return FrameParamKeyType.Unknown;
            }
        }

        /// <summary>
        /// Obtain a frame parameter definition instance given a parameter name
        /// </summary>
        /// <param name="paramName">Param key name</param>
        /// <returns><see cref="FrameParamDef"/> instance</returns>
        // ReSharper disable once UnusedMember.Global
        public static FrameParamDef GetParamDefByName(string paramName)
        {
            if (string.IsNullOrWhiteSpace(paramName))
                throw new ArgumentOutOfRangeException(nameof(paramName), "paramName is empty");

            var paramType = GetParamTypeByName(paramName);

            if (paramType == FrameParamKeyType.Unknown)
                throw new ArgumentOutOfRangeException(nameof(paramName), "unknown value for paramName: " + paramName);

            return GetParamDefByType(paramType);
        }

        /// <summary>
        /// Obtain a frame parameter definition instance given a parameter key type enum value
        /// </summary>
        /// <remarks>Will include the official parameter name, description, and data type for the given param key</remarks>
        /// <param name="paramType">Param key type enum</param>
        /// <returns><see cref="FrameParamDef"/> instance</returns>
        public static FrameParamDef GetParamDefByType(FrameParamKeyType paramType)
        {
            var targetType = GetFrameParamKeyDataType(paramType);

            switch (paramType)
            {
                case FrameParamKeyType.StartTimeMinutes:
                    return new FrameParamDef(FrameParamKeyType.StartTimeMinutes, "StartTime", targetType,
                                          "Start time of frame, in minutes");

                case FrameParamKeyType.DurationSeconds:
                    return new FrameParamDef(FrameParamKeyType.DurationSeconds,
                                          nameof(FrameParamKeyType.DurationSeconds), targetType,
                                          "Frame duration, in seconds");

                case FrameParamKeyType.Accumulations:
                    return new FrameParamDef(FrameParamKeyType.Accumulations, nameof(FrameParamKeyType.Accumulations),
                                          targetType,
                                          "Number of collected and summed acquisitions in a frame");

                case FrameParamKeyType.FrameType:
                    // Allowed values are defined by UIMFData.FrameType
                    return new FrameParamDef(FrameParamKeyType.FrameType, nameof(FrameParamKeyType.FrameType), targetType,
                                          "Frame Type: 0=MS (Legacy); 1=MS (Regular); 2=MS/MS (Frag); 3=Calibration; 4=Prescan");

                case FrameParamKeyType.Decoded:
                    // Allowed values are 0 or 1
                    return new FrameParamDef(FrameParamKeyType.Decoded, nameof(FrameParamKeyType.Decoded), targetType,
                                          "Tracks whether frame has been decoded: 0 for non-multiplexed or encoded; 1 if decoded");

                case FrameParamKeyType.CalibrationDone:
                    // Allowed values are 0 or 1, though -1 was used in the past instead of 0
                    return new FrameParamDef(FrameParamKeyType.CalibrationDone,
                                          nameof(FrameParamKeyType.CalibrationDone), targetType,
                                          "Tracks whether frame has been calibrated: 1 if calibrated");

                case FrameParamKeyType.Scans:
                    return new FrameParamDef(FrameParamKeyType.Scans, nameof(FrameParamKeyType.Scans), targetType,
                                          "Number of TOF scans in a frame");

                case FrameParamKeyType.MultiplexingEncodingSequence:
                    return new FrameParamDef(FrameParamKeyType.MultiplexingEncodingSequence,
                                          nameof(FrameParamKeyType.MultiplexingEncodingSequence), targetType,
                                          "The name of the sequence used to encode the data when acquiring multiplexed data");

                case FrameParamKeyType.MPBitOrder:
                    return new FrameParamDef(FrameParamKeyType.MPBitOrder, nameof(FrameParamKeyType.MPBitOrder), targetType,
                                          "Multiplexing bit order; Determines size of the bit sequence");

                case FrameParamKeyType.TOFLosses:
                    return new FrameParamDef(FrameParamKeyType.TOFLosses, nameof(FrameParamKeyType.TOFLosses), targetType,
                                          "Number of TOF Losses (lost/skipped scans due to I/O problems)");

                case FrameParamKeyType.AverageTOFLength:
                    return new FrameParamDef(FrameParamKeyType.AverageTOFLength,
                                          nameof(FrameParamKeyType.AverageTOFLength), targetType,
                                          "Average time between TOF trigger pulses, in nanoseconds");

                case FrameParamKeyType.CalibrationSlope:
                    return new FrameParamDef(FrameParamKeyType.CalibrationSlope,
                                          nameof(FrameParamKeyType.CalibrationSlope), targetType,
                                          "Calibration slope; k is slope / 10000");

                case FrameParamKeyType.CalibrationIntercept:
                    return new FrameParamDef(FrameParamKeyType.CalibrationIntercept,
                                          nameof(FrameParamKeyType.CalibrationIntercept), targetType,
                                          "Calibration intercept; t0 is intercept * 10000");

                case FrameParamKeyType.MassCalibrationCoefficienta2:
                    return new FrameParamDef(FrameParamKeyType.MassCalibrationCoefficienta2,
                                          nameof(FrameParamKeyType.MassCalibrationCoefficienta2), targetType,
                                          "a2 parameter for residual mass error correction; ResidualMassError = a2*t + b2*t^3 + c2*t^5 + d2*t^7 + e2*t^9 + f2*t^11");

                case FrameParamKeyType.MassCalibrationCoefficientb2:
                    return new FrameParamDef(FrameParamKeyType.MassCalibrationCoefficientb2,
                                          nameof(FrameParamKeyType.MassCalibrationCoefficientb2), targetType,
                                          "b2 parameter for residual mass error correction");

                case FrameParamKeyType.MassCalibrationCoefficientc2:
                    return new FrameParamDef(FrameParamKeyType.MassCalibrationCoefficientc2,
                                          nameof(FrameParamKeyType.MassCalibrationCoefficientc2), targetType,
                                          "c2 parameter for residual mass error correction");

                case FrameParamKeyType.MassCalibrationCoefficientd2:
                    return new FrameParamDef(FrameParamKeyType.MassCalibrationCoefficientd2,
                                          nameof(FrameParamKeyType.MassCalibrationCoefficientd2), targetType,
                                          "db2 parameter for residual mass error correction");

                case FrameParamKeyType.MassCalibrationCoefficiente2:
                    return new FrameParamDef(FrameParamKeyType.MassCalibrationCoefficiente2,
                                          nameof(FrameParamKeyType.MassCalibrationCoefficiente2), targetType,
                                          "e2 parameter for residual mass error correction");

                case FrameParamKeyType.MassCalibrationCoefficientf2:
                    return new FrameParamDef(FrameParamKeyType.MassCalibrationCoefficientf2,
                                          nameof(FrameParamKeyType.MassCalibrationCoefficientf2), targetType,
                                          "f2 parameter for residual mass error correction");

                case FrameParamKeyType.AmbientTemperature:
                    return new FrameParamDef(FrameParamKeyType.AmbientTemperature,
                                          nameof(FrameParamKeyType.AmbientTemperature), targetType,
                                          "Ambient temperature, in Celsius");

                case FrameParamKeyType.DriftTubeTemperature:
                    return new FrameParamDef(FrameParamKeyType.DriftTubeTemperature,
                                             nameof(FrameParamKeyType.DriftTubeTemperature), targetType,
                                             "Drift tube temperature, in Celsius");

                case FrameParamKeyType.VoltHVRack1:
                    return new FrameParamDef(FrameParamKeyType.VoltHVRack1, nameof(FrameParamKeyType.VoltHVRack1),
                                          targetType,
                                          "Volt hv rack 1");

                case FrameParamKeyType.VoltHVRack2:
                    return new FrameParamDef(FrameParamKeyType.VoltHVRack2, nameof(FrameParamKeyType.VoltHVRack2),
                                          targetType,
                                          "Volt hv rack 2");

                case FrameParamKeyType.VoltHVRack3:
                    return new FrameParamDef(FrameParamKeyType.VoltHVRack3, nameof(FrameParamKeyType.VoltHVRack3),
                                          targetType,
                                          "Volt hv rack 3");

                case FrameParamKeyType.VoltHVRack4:
                    return new FrameParamDef(FrameParamKeyType.VoltHVRack4, nameof(FrameParamKeyType.VoltHVRack4),
                                          targetType,
                                          "Volt hv rack 4");

                case FrameParamKeyType.VoltCapInlet:
                    return new FrameParamDef(FrameParamKeyType.VoltCapInlet, nameof(FrameParamKeyType.VoltCapInlet),
                                          targetType,
                                          "Capillary Inlet Voltage");

                case FrameParamKeyType.VoltEntranceHPFIn:
                    return new FrameParamDef(FrameParamKeyType.VoltEntranceHPFIn,
                                          nameof(FrameParamKeyType.VoltEntranceHPFIn), targetType,
                                          "HPF In Voltage");

                case FrameParamKeyType.VoltEntranceHPFOut:
                    return new FrameParamDef(FrameParamKeyType.VoltEntranceHPFOut,
                                          nameof(FrameParamKeyType.VoltEntranceHPFOut), targetType,
                                          "HPF Out Voltage");

                case FrameParamKeyType.VoltEntranceCondLmt:
                    return new FrameParamDef(FrameParamKeyType.VoltEntranceCondLmt,
                                          nameof(FrameParamKeyType.VoltEntranceCondLmt), targetType,
                                          "Entrance Cond Limit Voltage");

                case FrameParamKeyType.VoltTrapOut:
                    return new FrameParamDef(FrameParamKeyType.VoltTrapOut, nameof(FrameParamKeyType.VoltTrapOut),
                                          targetType,
                                          "Trap Out Voltage");

                case FrameParamKeyType.VoltTrapIn:
                    return new FrameParamDef(FrameParamKeyType.VoltTrapIn, nameof(FrameParamKeyType.VoltTrapIn), targetType,
                                          "Trap In Voltage");

                case FrameParamKeyType.VoltJetDist:
                    return new FrameParamDef(FrameParamKeyType.VoltJetDist, nameof(FrameParamKeyType.VoltJetDist),
                                          targetType,
                                          "Jet Disruptor Voltage");

                case FrameParamKeyType.VoltQuad1:
                    return new FrameParamDef(FrameParamKeyType.VoltQuad1, nameof(FrameParamKeyType.VoltQuad1), targetType,
                                          "Fragmentation Quadrupole Voltage 1");

                case FrameParamKeyType.VoltCond1:
                    return new FrameParamDef(FrameParamKeyType.VoltCond1, nameof(FrameParamKeyType.VoltCond1), targetType,
                                          "Fragmentation Conductance Voltage 1");

                case FrameParamKeyType.VoltQuad2:
                    return new FrameParamDef(FrameParamKeyType.VoltQuad2, nameof(FrameParamKeyType.VoltQuad2), targetType,
                                          "Fragmentation Quadrupole Voltage 2");

                case FrameParamKeyType.VoltCond2:
                    return new FrameParamDef(FrameParamKeyType.VoltCond2, nameof(FrameParamKeyType.VoltCond2), targetType,
                                          "Fragmentation Conductance Voltage 2");

                case FrameParamKeyType.VoltIMSOut:
                    return new FrameParamDef(FrameParamKeyType.VoltIMSOut, nameof(FrameParamKeyType.VoltIMSOut), targetType,
                                          "IMS Out Voltage");

                case FrameParamKeyType.VoltExitHPFIn:
                    return new FrameParamDef(FrameParamKeyType.VoltExitHPFIn, nameof(FrameParamKeyType.VoltExitHPFIn),
                                          targetType,
                                          "HPF In Voltage");

                case FrameParamKeyType.VoltExitHPFOut:
                    return new FrameParamDef(FrameParamKeyType.VoltExitHPFOut, nameof(FrameParamKeyType.VoltExitHPFOut),
                                          targetType,
                                          "HPF Out Voltage");

                case FrameParamKeyType.VoltExitCondLmt:
                    return new FrameParamDef(FrameParamKeyType.VoltExitCondLmt,
                                          nameof(FrameParamKeyType.VoltExitCondLmt), targetType,
                                          "Exit Cond Limit Voltage");

                case FrameParamKeyType.PressureFront:
                    return new FrameParamDef(FrameParamKeyType.PressureFront, nameof(FrameParamKeyType.PressureFront),
                                          targetType,
                                          "Pressure at front of Drift Tube");

                case FrameParamKeyType.PressureBack:
                    return new FrameParamDef(FrameParamKeyType.PressureBack, nameof(FrameParamKeyType.PressureBack),
                                          targetType,
                                          "Pressure at back of Drift Tube");

                case FrameParamKeyType.HighPressureFunnelPressure:
                    return new FrameParamDef(FrameParamKeyType.HighPressureFunnelPressure,
                                          nameof(FrameParamKeyType.HighPressureFunnelPressure), targetType,
                                          "High pressure funnel pressure");

                case FrameParamKeyType.IonFunnelTrapPressure:
                    return new FrameParamDef(FrameParamKeyType.IonFunnelTrapPressure,
                                          nameof(FrameParamKeyType.IonFunnelTrapPressure), targetType,
                                          "Ion funnel trap pressure");

                case FrameParamKeyType.RearIonFunnelPressure:
                    return new FrameParamDef(FrameParamKeyType.RearIonFunnelPressure,
                                          nameof(FrameParamKeyType.RearIonFunnelPressure), targetType,
                                          "Rear ion funnel pressure");

                case FrameParamKeyType.QuadrupolePressure:
                    return new FrameParamDef(FrameParamKeyType.QuadrupolePressure,
                                          nameof(FrameParamKeyType.QuadrupolePressure), targetType,
                                          "Quadrupole pressure");

                case FrameParamKeyType.ESIVoltage:
                    return new FrameParamDef(FrameParamKeyType.ESIVoltage, nameof(FrameParamKeyType.ESIVoltage), targetType,
                                          "ESI voltage");

                case FrameParamKeyType.FloatVoltage:
                    return new FrameParamDef(FrameParamKeyType.FloatVoltage, nameof(FrameParamKeyType.FloatVoltage),
                                          targetType,
                                          "Float voltage");

                case FrameParamKeyType.FragmentationProfile:
                    return new FrameParamDef(FrameParamKeyType.FragmentationProfile,
                                          nameof(FrameParamKeyType.FragmentationProfile), targetType,
                                          "Voltage profile used in fragmentation (array of doubles, converted to an array of bytes, then stored as a Base 64 encoded string)");

                case FrameParamKeyType.ScanNumFirst:
                    return new FrameParamDef(FrameParamKeyType.ScanNumFirst, nameof(FrameParamKeyType.ScanNumFirst),
                                          targetType,
                                          "First scan");

                case FrameParamKeyType.ScanNumLast:
                    return new FrameParamDef(FrameParamKeyType.ScanNumLast, nameof(FrameParamKeyType.ScanNumLast),
                                          targetType,
                                          "Last scan");

                case FrameParamKeyType.PressureUnits:
                    return new FrameParamDef(FrameParamKeyType.PressureUnits, nameof(FrameParamKeyType.PressureUnits),
                                          targetType,
                                          "Units for pressure");

                default:
                    throw new ArgumentOutOfRangeException(nameof(paramType), "Unrecognized frame param enum for paramType: " + (int)paramType);
            }
        }
    }
}
