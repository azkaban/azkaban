Azkaban SPI
========
The SPI module is the contract between other Azkaban sub-modules. It mainly contains the top level interfaces and 
exceptions that are shared across the entire application.

##Dependencies
The SPI package is ideally intended to be kept DEPENDENCY FREE (or have a VERY MINIMAL set) so that other 
components can easily depend on it.