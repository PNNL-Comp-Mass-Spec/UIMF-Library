// --------------------------------------------------------------------------------------------------------------------
// <summary>
//   Defines the frame type info.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;

namespace UIMFLibrary
{
    /// <summary>
    /// Class for tracking frame index of each frame number defined in a .UIMF file
    /// </summary>
    internal class FrameSetContainer
    {
        #region Constructors and Destructors

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="numFramesInFile">
        /// Number of frames in the file.
        /// </param>
        public FrameSetContainer(int numFramesInFile)
        {
            NumFrames = 0;
            FrameIndexes = new Dictionary<int, int>(numFramesInFile+1);
        }

        #endregion

        #region Public Properties

        /// <summary>
        /// Mapping between frame number and frame index
        /// </summary>
        /// <remarks>Key is frame number, value is frame index</remarks>
        public Dictionary<int, int> FrameIndexes { get; }

        /// <summary>
        /// Gets the num frames.
        /// </summary>
        public int NumFrames { get; private set; }

        #endregion

        #region Public Methods and Operators

        /// <summary>
        /// Add a frame.
        /// </summary>
        /// <param name="frameNumber">
        /// Frame number.
        /// </param>
        public void AddFrame(int frameNumber)
        {
            if (FrameIndexes.ContainsKey(frameNumber))
                throw new Exception("Frame " + frameNumber + " was sent to FrameSetContainer.AddFrame more than 1 time; likely a programming bug");

            FrameIndexes.Add(frameNumber, NumFrames);
            NumFrames++;
        }

        #endregion
    }
}