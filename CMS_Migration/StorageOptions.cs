using System;
using System.Collections.Generic;
using System.Text;

namespace CMS_Migration
{
    public class StorageOptions
    {
        public string LiveConnectionString { get; set; }
        public string UatConnectionString { get; set; }
        public string ContentFolderPath { get; set; }
        public string AlternativeContentFolderPath { get; set; }
    }
}
