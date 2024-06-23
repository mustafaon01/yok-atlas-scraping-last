import pandas as pd

file_path = '2021.xlsx'
df = pd.read_excel(file_path)

program_kodlari = df.iloc[2:, 0]
program_kodlari_list = program_kodlari.tolist()
print(len(program_kodlari_list))

with open('2021.txt', 'w') as file:
    for item in program_kodlari_list:
        file.write(f"{item}\n")

print("Liste txt dosyasına başarıyla yazıldı.")