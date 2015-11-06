package sereno

import "github.com/sony/sonyflake"

var sf *sonyflake.Sonyflake

func init() {
	var st sonyflake.Settings
	//TODO integrate google's instance id if on GCE...
	// https://github.com/GoogleCloudPlatform/gcloud-golang/blob/master/compute/metadata/metadata.go#L155
	// https://github.com/GoogleCloudPlatform/gcloud-golang/blob/master/compute/metadata/metadata.go#L250
	// if metadata.OnGCE() {
	//     st.MachineID = func(){ return toIn16( metadata.InstanceId() ) }
	// }
	sf = sonyflake.NewSonyflake(st)
	if sf == nil {
		panic("sonyflake not created")
	}
}

func NextId() (uint64, error) {
	return sf.NextID()
}
