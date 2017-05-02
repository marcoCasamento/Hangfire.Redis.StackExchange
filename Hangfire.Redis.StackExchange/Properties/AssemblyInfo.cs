using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

// General Information about an assembly is controlled through the following 
// set of attributes. Change these attribute values to modify the information
// associated with an assembly.
[assembly: InternalsVisibleTo("Hangfire.Redis.Tests")]
[assembly: InternalsVisibleTo("Hangfire.Tests")]

// Allow the generation of mocks for internal types
[assembly: InternalsVisibleTo("DynamicProxyGenAssembly2")]