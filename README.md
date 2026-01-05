# uslci-transport
Calculation of average transport distances and modes for US commodities. 

# How to use
Required for environment:
- [flcac-utils](https://github.com/FLCAC-admin/flcac-utils/blob/main/flcac_utils/generate_processes.py)
  - pip install git+https://github.com/FLCAC-admin/flcac-utils.git
- [esupy](https://github.com/USEPA/esupy/tree/e0464c50701001501ba7bc71a608a26ebc0c2688)
  - pip install git+https://github.com/USEPA/esupy.git

Scripts to run:
- (1) Run [calculate_transport_distances.py](https://github.com/FLCAC-admin/uslci-transport/blob/main/calculate_transport_distances.py)
- (2) Run [build_transportation_olca_objects.py](https://github.com/FLCAC-admin/uslci-transport/blob/main/build_transportation_olca_objects.py)

Output:
- Import the zip file created in the output folder into openLCA 2.x.x
- This transport data set needs to be imported into a database containing USLCI so that default providers are linked
- Reference the [2017 CFS Commodity Code (SCTG) Manual](https://www.bts.gov/surveys/commodity-flow-survey/2017-cfs-commodity-code-sctg-manual) to determine which commodity process should be selected

## Datasets

| Datasts             | Version | flcac-utils | Release        | Documentation |
|---------------------|---------|-------------|----------------|---------------|
| Commodity Transport | v1.0.0  | v0.2.0      | 2025 Q4, USLCI | [Methods](https://github.com/FLCAC-admin/uslci-transport/releases/download/v1.0.0/USLCI_commodity_transport_v1.pdf) |
