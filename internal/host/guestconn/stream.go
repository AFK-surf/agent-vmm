package guestconn

func StreamReceiveResult(channel <-chan error) error {
	select {
	case err := <-channel:
		return err
	default:
		return nil
	}
}
