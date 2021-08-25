using System;
using System.Collections.Generic;

// ReSharper disable UnusedMember.Global

namespace UIMFLibrary
{
    /// <summary>
    /// Global parameters container
    /// </summary>
    public class GlobalParams
    {
        #region Properties

        /// <summary>
        /// Global parameters dictionary
        /// </summary>
        /// <remarks>Key is parameter type; value is the global parameter container (<see cref="GlobalParam"/> class)</remarks>
        public Dictionary<GlobalParamKeyType, GlobalParam> Values { get; }

        /// <summary>
        /// Total number of TOF bins in frame
        /// </summary>
        public int Bins
        {
            get
            {
                if (!HasParameter(GlobalParamKeyType.Bins))
                    return 0;

                return GetValueInt32(GlobalParamKeyType.Bins);
            }
        }

        /// <summary>
        /// Width of TOF bins (in ns)
        /// </summary>
        public double BinWidth {
            get
            {
                if (!HasParameter(GlobalParamKeyType.BinWidth))
                    return 0;

                return GetValueDouble(GlobalParamKeyType.BinWidth);
            }
        }

        /// <summary>
        /// Returns True if storing data using the ppm bin-based mode
        /// </summary>
        public bool IsPpmBinBased
        {
            get
            {
                var instrumentClass = GetValueInt32(GlobalParamKeyType.InstrumentClass, (int)InstrumentClassType.TOF);
                return instrumentClass == (int)InstrumentClassType.PpmBinBased;
            }
        }

        /// <summary>
        /// Number of frames in the dataset
        /// </summary>
        public int NumFrames
        {
            get
            {
                if (!HasParameter(GlobalParamKeyType.NumFrames))
                    return 0;

                return GetValueInt32(GlobalParamKeyType.NumFrames);
            }
        }

        /// <summary>
        /// TOF correction time
        /// </summary>
        public float TOFCorrectionTime
        {
            get
            {
                if (!HasParameter(GlobalParamKeyType.TOFCorrectionTime))
                    return 0;

                return (float)GetValueDouble(GlobalParamKeyType.TOFCorrectionTime);
            }
        }

        #endregion

        /// <summary>
        /// Constructor
        /// </summary>
        public GlobalParams()
        {
            Values = new Dictionary<GlobalParamKeyType, GlobalParam>();
        }

        /// <summary>
        /// Add or update a parameter's value
        /// </summary>
        /// <param name="paramType">Parameter type</param>
        /// <param name="value">Value (double)</param>
        public GlobalParams AddUpdateValue(GlobalParamKeyType paramType, double value)
        {
            return AddUpdateValueDynamic(paramType, value);
        }

        /// <summary>
        /// Add or update a parameter's value
        /// </summary>
        /// <param name="paramType">Parameter type</param>
        /// <param name="value">Value (int)</param>
        public GlobalParams AddUpdateValue(GlobalParamKeyType paramType, int value)
        {
            return AddUpdateValueDynamic(paramType, value);
        }

        /// <summary>
        /// Add or update a parameter's value
        /// </summary>
        /// <param name="paramType"></param>
        /// <param name="value"></param>
        public GlobalParams AddUpdateValue(GlobalParamKeyType paramType, DateTime value)
        {
            return AddUpdateValueDynamic(paramType, UIMFDataUtilities.StandardizeDate(value));
        }

        /// <summary>
        /// Add or update a parameter's value
        /// </summary>
        /// <param name="paramType">Parameter type</param>
        /// <param name="value">Value (string)</param>
        public GlobalParams AddUpdateValue(GlobalParamKeyType paramType, string value)
        {
            return AddUpdateValueDynamic(paramType, value);
        }

        /// <summary>
        /// Add or update a parameter's value
        /// </summary>
        /// <param name="paramType">Parameter type</param>
        /// <param name="value">Value (dynamic)</param>
        public GlobalParams AddUpdateValue(GlobalParamKeyType paramType, dynamic value)
        {
            return AddUpdateValueDynamic(paramType, value);
        }

        /// <summary>
        /// Add or update a parameter's value
        /// </summary>
        /// <param name="paramType">Parameter type</param>
        /// <param name="value">Value (string)</param>
        private GlobalParams AddUpdateValueDynamic(GlobalParamKeyType paramType, dynamic value)
        {
            if (Values.TryGetValue(paramType, out var paramEntry))
            {
                paramEntry.Value = value;
            }
            else
            {
                paramEntry = new GlobalParam(paramType, value);
                Values.Add(paramType, paramEntry);
            }

            return this;
        }

        /// <summary>
        /// Get the value for a parameter
        /// </summary>
        /// <remarks>Returns an empty string if not defined</remarks>
        /// <param name="paramType">Parameter type</param>
        /// <returns>Value (dynamic)</returns>
        public dynamic GetValue(GlobalParamKeyType paramType)
        {
            var defaultValue = GlobalParamUtilities.GetDefaultValueByType(paramType);
            return GetValue(paramType, defaultValue);
        }

        /// <summary>
        /// Get the value for a parameter
        /// </summary>
        /// <param name="paramType">Parameter type</param>
        /// <param name="valueIfMissing">Value to return if the parameter is not defined</param>
        /// <returns>Value (dynamic)</returns>
        public dynamic GetValue(GlobalParamKeyType paramType, dynamic valueIfMissing)
        {
            if (Values.TryGetValue(paramType, out var paramEntry))
            {
                return paramEntry.Value;
            }

            return valueIfMissing;
        }

        /// <summary>
        /// Get the value for a parameter
        /// </summary>
        /// <remarks>Returns 0 if not defined</remarks>
        /// <param name="paramType">Parameter type</param>
        /// <returns>Value (double)</returns>
        public double GetValueDouble(GlobalParamKeyType paramType)
        {
            return GetValueDouble(paramType, 0.0);
        }

        /// <summary>
        /// Get the value for a parameter
        /// </summary>
        /// <param name="paramType">Parameter type</param>
        /// <param name="valueIfMissing">Value to return if the parameter is not defined</param>
        /// <returns>Value (double)</returns>
        public double GetValueDouble(GlobalParamKeyType paramType, double valueIfMissing)
        {
            if (Values.TryGetValue(paramType, out var paramEntry))
            {
                if (FrameParamUtilities.ConvertDynamicToDouble(paramEntry.Value, out double result))
                    return result;
            }

            return valueIfMissing;
        }

        /// <summary>
        /// Get the value for a parameter
        /// </summary>
        /// <remarks>Returns 0 if not defined</remarks>
        /// <param name="paramType">Parameter type</param>
        /// <returns>Value (int)</returns>
        public int GetValueInt32(GlobalParamKeyType paramType)
        {
            return GetValueInt32(paramType, 0);
        }

        /// <summary>
        /// Get the value for a parameter
        /// </summary>
        /// <param name="paramType">Parameter type</param>
        /// <param name="valueIfMissing">Value to return if the parameter is not defined</param>
        /// <returns>Value (int)</returns>
        public int GetValueInt32(GlobalParamKeyType paramType, int valueIfMissing)
        {
            if (Values.TryGetValue(paramType, out var paramEntry))
            {
                if (FrameParamUtilities.ConvertDynamicToInt32(paramEntry.Value, out int result))
                    return result;
            }

            return valueIfMissing;
        }

        /// <summary>
        /// Get the value for a parameter
        /// </summary>
        /// <param name="paramType">Parameter type</param>
        /// <param name="valueIfMissing">Value to return if the parameter is not defined</param>
        /// <returns>Value (string)</returns>
        public string GetValueString(GlobalParamKeyType paramType, string valueIfMissing = "")
        {
            return GetValue(paramType, valueIfMissing);
        }

        /// <summary>
        /// Lookup whether or not a global parameter is defined
        /// </summary>
        /// <param name="paramType">Parameter type</param>
        /// <returns>True if defined, otherwise false</returns>
        public bool HasParameter(GlobalParamKeyType paramType)
        {
            return Values.ContainsKey(paramType);
        }
    }
}
