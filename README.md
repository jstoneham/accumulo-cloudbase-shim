accumulo-cloudbase-shim
=======================

Allows programming to the Accumulo 1.4 API and running against Cloudbase 1.3. Provides a drop-in replacement for accumulo-core by implementing (enough of) the Accumulo API to provide a usable client surface area, but wrapping real Cloudbase classes. Based on the Accumulo 1.4.1 API. The idea is to compile against accumulo-core, but ship either accumulo-core or accumulo-cloudbase-shim depending on your deployment environment.

Does not provide the COMPLETE surface area of the Accumulo API, only what has been needed/added so far.

Contains some limited logic for translation of iterators - handles classname translation and a few parameter translations.

Most of the classes provide a public member 'impl' whereby you can reach the real Cloudbase version of the object, so if you should need to write some Cloudbase-specific code, you can get into the actual Cloudbase stuff. Of course you'll need to protect yourself from ClassNotFoundExceptions with careful use of reflection for the cases where you run with Accumulo.
