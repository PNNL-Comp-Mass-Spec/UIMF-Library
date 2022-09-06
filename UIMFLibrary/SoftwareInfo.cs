using System;

namespace UIMFLibrary
{
    /// <summary>
    /// Information about software that has modified the UIMF file
    /// </summary>
    public class SoftwareInfo
    {
        /// <summary>
        /// Software row ID
        /// </summary>
        public int Id { get; set; }

        /// <summary>
        /// Name of the software
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Type of software (acquisition, conversion, post-processing)
        /// </summary>
        public string SoftwareType { get; set; }

        /// <summary>
        /// Note: Can be a note on what the software did.
        /// </summary>
        public string Note { get; set; }

        /// <summary>
        /// Version of the software
        /// </summary>
        public Version SoftwareVersion { get; set; }

        /// <summary>
        /// Last modified date of the software executable
        /// </summary>
        public DateTime SoftwareExeDate { get; set; }

        /// <summary>
        /// Date the row was entered
        /// </summary>
        public DateTime DateEntered { get; set; }

        /// <summary>
        /// ToString() override
        /// </summary>
        public override string ToString()
        {
            return $"{Name} {SoftwareType}";
        }
    }
}
