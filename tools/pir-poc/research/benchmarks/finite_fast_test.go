package pir
import("bytes";"testing")
func TestColdFastEncodingEqualsReference(t *testing.T) {
 for _,pair:=range [][2]int{{8,3},{10,5},{12,3}} {
  for _,width:=range []int{1,7,32} {
   p:=&Params{M:pair[0],D:pair[1],N:16,Record_len:width}
   db:=&Database{Num_records:16,Record_len:width,Data:make([]byte,16*width)}
   for i:=range db.Data {db.Data[i]=byte(i*37+11)}
   a,b:=EncodeDatabase(db,p),ColdEncodeFast(db,p)
   if !bytes.Equal(a.Data,b.Data) {t.Fatal("optimized encoding differs from reference",pair,width)}
  }
 }
}
