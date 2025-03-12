# marc_db

A simple database and query interface for the microbial archive

## Schema description

Isolates

isolate_id: needs to be Unique. The sequencing lab is currently in charge of the numbering system, but we need to double check to make sure they are unique (Throw and error if there are duplicates for now.)

subject_id: Unique ID created for each MRN, obtained from program 1.

specimen_id: Unique ID created for each specimen barcode, obtained from program 1.

source: Blood, oral swab etc

suspected_organism: Can be labeled as "unknown" which is distinct from NA or left blank

special_collection: can be empty. If these isolates are collected under the purview of a project, specify it here (like bacteremia / NICU)

received_date

cryobanking_date

 

 

Aliquots

The isolates are aliquoted and kept in tubes in the freezer.

The table will be populated from the columns Tube barcode a/b/c and Box-name_position a/b/c. We don’t need the bead tube barcodes. These hold the barcodes of the glycerol stocks of the isolates. You need both tube barcode and box name to locate an isolate. An isolate can have more than one aliquot (a/b/c). For example old isolates have 3 aliquots, the newer ones only have 2. These would be multiple entries in the database with the same isolate_id.

isolate_id

tube_barcode

box_name

 

 

Once the above database is set, let’s touch base and talk about next steps. But just as a preview, we eventually want to add more tables:

 

Sequencing

This will point us to which run the isolates are in. Some isolates may be run more than once. Those would be new entries in the database.

isolate_id

sample_registry_id (one isolate may be run multiple times) The runs are also registered per lane. We only need to register the first lane

 

 

AssemblyStats

These are summary results from the pipeline. This is probably the most rough draft of the schema. These results will most likely be represented in multiple tables.

Isolate_id

sga_output_id (create a unique ID by concating isolateID and entry number or timestamp (?) e.g. marc.bacteremia.1017.a_1)

time_stamp (when was it created)

config_file (information on databases on softwares sga_extension_version (add it to config file)

assembly_fasta_path: Where do we store the assembly fastq file?

N50

# of contigs

species

contaminations

Other outputs as we finalize the sga pipeline

 

MLST

sga_output_id

schema

sequence_type

alleles

 

AbxResistance

sga_output_id

antibiotics

gene_name

 

And most likely a few more tables to contain the data from other software.