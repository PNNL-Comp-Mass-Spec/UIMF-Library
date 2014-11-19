using System;
using System.Collections.Generic;
using System.Globalization;

namespace UIMFLibrary
{
   
    /// <summary>
    /// Global parameters container
    /// </summary>
    public class GlobalParams
    {
        #region Member Variables

        /// <summary>
        /// Global parameters dictionary
        /// </summary>
        /// <remarks>Key is parameter type; value is the Global parameter container (<see cref="GlobalParam"/> class)</remarks>
        private readonly Dictionary<GlobalParamKeyType, GlobalParam> mGlobalParameters;

        #endregion

        #region Properties

        /// <summary>
        /// Global parameters dictionary
        /// </summary>
        /// <remarks>Key is parameter type; value is the global parameter container (<see cref="GlobalParam"/> class)</remarks>
        public Dictionary<GlobalParamKeyType, GlobalParam> Values
        {
            get { return mGlobalParameters; }
        }

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
            mGlobalParameters = new Dictionary<GlobalParamKeyType, GlobalParam>();
        }

        /// <summary>
        /// Add or update a parameter's value
        /// </summary>
        /// <param name="paramType">Parameter type</param>
        /// <param name="value">Value (double)</param>
        public void AddUpdateValue(GlobalParamKeyType paramType, double value)
        {
            AddUpdateValue(paramType, value.ToString(CultureInfo.InvariantCulture));
        }

        /// <summary>
        /// Add or update a parameter's value
        /// </summary>
        /// <param name="paramType">Parameter type</param>
        /// <param name="value">Value (int)</param>
        public void AddUpdateValue(GlobalParamKeyType paramType, int value)
        {
            AddUpdateValue(paramType, value.ToString(CultureInfo.InvariantCulture));
        }

        /// <summary>
        /// Add or update a parameter's value
        /// </summary>
        /// <param name="paramType">Parameter type</param>
        /// <param name="value">Value (string)</param>
        public void AddUpdateValue(GlobalParamKeyType paramType, string value)
        {
            GlobalParam paramEntry;
            if (mGlobalParameters.TryGetValue(paramType, out paramEntry))
            {
                paramEntry.Value = value;
            }
            else
            {
                paramEntry = new GlobalParam(paramType, value);
                mGlobalParameters.Add(paramType, paramEntry);
            }
        }

        /// <summary>
        /// Get the value for a parameter
        /// </summary>
        /// <param name="paramType">Parameter type</param>
        /// <returns>Value (string)</returns>
        /// <remarks>Returns an empty string if not defined</remarks>
        public string GetValue(GlobalParamKeyType paramType)
        {
            return GetValue(paramType, string.Empty);
        }

        /// <summary>
        /// Get the value for a parameter
        /// </summary>
        /// <param name="paramType">Parameter type</param>
        /// <param name="valueIfMissing">Value to return if the parameter is not defined</param>
        /// <returns>Value (string)</returns>
        public string GetValue(GlobalParamKeyType paramType, string valueIfMissing)
        {
            GlobalParam paramEntry;
            if (mGlobalParameters.TryGetValue(paramType, out paramEntry))
            {
                return paramEntry.Value;
            }

            return valueIfMissing;
        }

        /// <summary>
        /// Get the value for a parameter
        /// </summary>
        /// <param name="paramType">Parameter type</param>
        /// <returns>Value (double)</returns>
        /// <remarks>Returns 0 if not defined</remarks>
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
            GlobalParam paramEntry;
            if (mGlobalParameters.TryGetValue(paramType, out paramEntry))
            {
                double result;
                if (Double.TryParse(paramEntry.Value, out result))
                    return result;
            }

            return valueIfMissing;
        }

        /// <summary>
        /// Get the value for a parameter
        /// </summary>
        /// <param name="paramType">Parameter type</param>
        /// <returns>Value (int)</returns>
        /// <remarks>Returns 0 if not defined</remarks>
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
            GlobalParam paramEntry;
            if (mGlobalParameters.TryGetValue(paramType, out paramEntry))
            {
                int result;
                if (Int32.TryParse(paramEntry.Value, out result))
                    return result;
            }

            return valueIfMissing;
        }

        /// <summary>
        /// Lookup whether or not a global parameter is defined
        /// </summary>
        /// <param name="paramType">Parameter type</param>
        /// <returns>True if defined, otherwise false</returns>
        public bool HasParameter(GlobalParamKeyType paramType)
        {
            return mGlobalParameters.ContainsKey(paramType);
        }
    }
}
