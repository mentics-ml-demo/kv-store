CREATE TABLE IF NOT EXISTS label (
    version int not null,
    event_id int not null,
    timestamp int not null,
    offset_from int not null,
    offset_to int not null,
    label blob not null,
    PRIMARY KEY(version, event_id)
);

CREATE TABLE IF NOT EXISTS train (
    version int not null,
    event_id int not null,
    timestamp int not null,
    offset int not null,
    loss real not null,
    input blob not null,
    output blob not null,
    PRIMARY KEY(version, event_id)
);

CREATE INDEX train_loss_idx ON train (version, loss);
