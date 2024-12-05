package register

import (
	_ "beats/sender/file"
	_ "beats/sender/kafka"
	_ "beats/sender/socket"
	_ "beats/source/kafka"
)
