# create report from metadata/metadata.csv

# save a pdf in metadata/reports/DATE.pdf


def render():
    print("report script")

    # catalog_df['review'] = catalog_df[(catalog_df['end_date'] < catalog_df['date']) | (catalog_df['start_date'] > catalog_df['date']) | (catalog_df['date'].isna()) | (catalog_df['author'].isna()) | (catalog_df['title'].isna())]

    # cumulus_review.to_csv('cumulus_review.csv', index=False)
