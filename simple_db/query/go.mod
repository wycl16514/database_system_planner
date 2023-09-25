module query

replace file_manager => ../file_manager

replace log_manager => ../log_manager

replace buffer_manager => ../buffer_manager

replace tx => ../tx

replace record_manager => ../record_manager

go 1.19

require record_manager v0.0.0-00010101000000-000000000000

require (
	buffer_manager v0.0.0-00010101000000-000000000000 // indirect
	file_manager v0.0.0-00010101000000-000000000000 // indirect
	github.com/bzick/tokenizer v1.3.0 // indirect
	log_manager v0.0.0-00010101000000-000000000000 // indirect
	tx v0.0.0-00010101000000-000000000000 // indirect
)
