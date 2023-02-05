# AdvancedDB Project
## Σύντομη περιγραφή
Το παρόν Project υλοποιήθηκε στο πλαίσιο του μάθηματος "Προχωρημένα Θέματα Βάσεων Δεδομένων" του 9ου εξαμήνου. Η εκφώνηση της εργασίας βρίσκεται [εδώ](https://helios.ntua.gr/pluginfile.php/175280/mod_resource/content/2/AdvancedDB_project_2022.pdf).

## Περιγραφή των αρχείων
Ο κώδικας για τα queries υπάρχει σε 2 μορφές αρχείων, δηλαδή ως Python script [AdvancedDB_Project.py](AdvancedDB_Project/AdvancedDB_Project.py) και ως Jupyter Notebook αρχείο [AdvancedDB_Project.pynb](AdvancedDB_Project/AdvancedDB_Project.pynb). Οι κώδικες των αρχείων αυτών είναι ισοδύναμοι από άποψη πληρότητας.
Το αρχείο [workers.sh](AdvancedDB_Project/workers.sh) περιλαμβάνει εντολές για την ενεργοποίηση και την απενεργοποίηση όλων των εργατών.

## Εκτέλεση
- [AdvancedDB_Project.py](AdvancedDB_Project/AdvancedDB_Project.py)
Μπορεί να εκτελεστεί με τη βοήθεια της εντολής:
```bash
python3.8 AdvancedDB_Project.py
```
- [AdvancedDB_Project.pynb](AdvancedDB_Project/AdvancedDB_Project.pynb)
Μπορεί να εκτελεστεί με τη βοήθεια της εντολής:
```bash
jupyter-lab --no-browser --ip="83.212.80.22"
```
- [workers.sh](AdvancedDB_Project/workers.sh)
Μπορεί να εκτελεστεί στο master machine με τη βοήθεια της εντολής:
```bash
./workers.sh [start|stop] [1|2]
```
