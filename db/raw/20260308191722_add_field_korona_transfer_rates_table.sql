-- +goose Up
-- +goose StatementBegin
ALTER TABLE raw.korona_transfer_rates
ADD COLUMN new_field VARCHAR(10);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE raw.korona_transfer_rates
DROP COLUMN new_field;
-- +goose StatementEnd