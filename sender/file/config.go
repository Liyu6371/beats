package file

type Config struct {
	Enabled    bool `mapstructure:"enabled"`
	AppendMode bool `mapstructure:"append_mode"`
	Buffer     int  `mapstructure:"buffer"`
}
