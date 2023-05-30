


# %%
# Merging twitter probabilities and ticker prices
df_merge = pd.merge(index_funds_df, df_wide_wknd_merge, how='inner', on='date').set_index('date').fillna(0)
df_merge_original = df_merge.copy()

# Export twitter index fund merge
df_to_csv(df = df_merge, 
          folder = f'./data/merge/combined', 
          file = f'/tickers_and_twitter_users.csv')
index_funds_df.head(5)

# %%
# Latest Data
todays_test = download_todays_test(ticker_df, df_wide_wknd_merge, df_merge_original)
# Export latest test
df_to_csv(df = todays_test, 
          folder = f'./data/merge/combined', 
          file = f'/todays_test.csv')
if(len(todays_test) > 0):
    todays_test.head(5)
    


# %%

# Todays Data
todays_test = download_todays_test(ticker_df, df_wide, df_merge_original)
Xnew = sm.add_constant(todays_test, has_constant='add')

model = {} # Model Build For Each index fund
print(f"date: { todays_test.index.date.max() }")
output = pd.DataFrame(columns=['index', 'prediction'])
for t in ticker_df.ticker_label:
    data_with_target = create_target(df_merge.copy(), day = 5, ticker = t)
    m = linear_model(data_with_target,split=0.20,summary = False)
    y_pred = m['lm'].predict(Xnew)
    model[t] = (y_pred, m)
    output = pd.concat([output, pd.DataFrame.from_records([(t, y_pred[0])], columns=['index', 'prediction'])])
    
pd.set_option('display.max_rows', 500)
display(output.sort_values(by=['prediction'], ascending=False))