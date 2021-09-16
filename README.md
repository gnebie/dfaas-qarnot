# Qarnot DFaaS Payload

### Content :

* README.md

## MVP Simswap 

This is an MVP for running Simswap, a DeepFake framework, on Qarnot through a jupyter notebook with minimal user intervention using a GUI based on jupyter widgets.


Binder :   
[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/gnebie/dfaas-qarnot/simswap)


### Content :

* `simswap.ipynb` : notebook with graphical interface to launch the task on Qarnot.
* `postBuild`: post build file specific to Binder, used to set the notebook as trusted on launch.
* `requirements.txt`: pip requirements file of python modules needed for the use case.


### How to run it:

```bash
jupyter notebook simswap.ipynb
```