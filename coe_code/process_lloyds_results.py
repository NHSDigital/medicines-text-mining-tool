"""process Lloyds results"""

# %%
import logging
import pandas as pd

logging.basicConfig(level=logging.INFO)

# %%
MATCH_FILE = "lloyds_matched.csv"
matched = pd.read_csv(MATCH_FILE)
logging.info("Loaded %s", MATCH_FILE)

# %% How many matches
logging.info("%d were matched", len(matched))
# And at what levels
for level in matched['id_level'].unique():
    logging.info("%s -> %d", level, len(matched[matched['id_level'] == level]))
# %% How were the matched made
for level in matched['match_level'].unique():
    logging.info("%s -> %d", level, len(matched[matched['match_level'] == level]))
 
# %%
UNMAP_FILE = "lloyds_unmappable.csv"
unmap = pd.read_csv(UNMAP_FILE)
logging.info("Loaded %s", UNMAP_FILE)
logging.info("%d were unmappable", len(unmap))
# %%
