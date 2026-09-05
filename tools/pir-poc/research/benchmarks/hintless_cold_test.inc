// Append to the pinned hintless_simplepir_test.cc. Same upstream cryptographic
// parameters, complete synthetic hash-tag records, fresh Client per predicate.
#include <ctime>
#include <fstream>
#include <cstdlib>
#include <algorithm>
namespace hintless_pir { namespace hintless_simplepir { namespace {
double ColdProcessMs() { timespec t; clock_gettime(CLOCK_PROCESS_CPUTIME_ID,&t); return t.tv_sec*1000.0+t.tv_nsec/1e6; }
TEST(HintlessColdSearch, CompleteTagRecords) {
  Parameters params=kParameters;
  params.db_record_bit_size=256;
  double start=ColdProcessMs();
  ASSERT_OK_AND_ASSIGN(auto server,Server::Create(params));
  std::vector<std::string> entries;
  std::vector<uint64_t> directory;
  for(uint64_t i=0;i<64;i++) {
    uint64_t tag=i*2654435761ULL+17;
    directory.push_back(tag);
    std::string record(32,'\0');
    for(int j=0;j<8;j++)record[j]=char(tag>>(j*8));
    for(int j=8;j<32;j++)record[j]=char(i+j);
    ASSERT_OK(server->GetDatabase()->Append(record));entries.push_back(record);
  }
  ASSERT_OK(server->Preprocess());
  double setup_ms=ColdProcessMs()-start;
  auto pp=server->GetPublicParams();
  const char* output=std::getenv("PIR_COLD_HINTLESS_OUTPUT");
  std::ofstream file(output?output:"cold-hintless.jsonl");
  file<<"{\"phase\":\"global\",\"server_setup_cpu_ms\":"<<setup_ms<<",\"metadata_bytes\":"<<pp.ByteSizeLong()+directory.size()*8<<"}\n";
  for(int q=0;q<4;q++) {
    uint64_t key=directory[q*11];if(q==0)key++;
    start=ColdProcessMs();
    auto at=std::find(directory.begin(),directory.end(),key);
    int index=at==directory.end()?0:int(at-directory.begin());
    ASSERT_OK_AND_ASSIGN(auto client,Client::Create(params,pp));
    ASSERT_OK_AND_ASSIGN(auto request,client->GenerateRequest(index));
    std::string req_bytes=request.SerializeAsString();
    double client_ms=ColdProcessMs()-start;
    start=ColdProcessMs();
    HintlessPirRequest wire_request;ASSERT_TRUE(wire_request.ParseFromString(req_bytes));
    ASSERT_OK_AND_ASSIGN(auto response,server->HandleRequest(wire_request));
    std::string rsp_bytes=response.SerializeAsString();
    double server_ms=ColdProcessMs()-start;
    start=ColdProcessMs();
    HintlessPirResponse wire_response;ASSERT_TRUE(wire_response.ParseFromString(rsp_bytes));
    ASSERT_OK_AND_ASSIGN(auto result,client->RecoverRecord(wire_response));
    ASSERT_EQ(result,entries[index]);
    uint64_t tag=0;for(int j=0;j<8;j++)tag|=uint64_t(uint8_t(result[j]))<<(8*j);
    ASSERT_EQ(tag==key,q!=0);
    client_ms+=ColdProcessMs()-start;
    file<<"{\"phase\":\"query\",\"fresh_client\":"<<q<<",\"server_cpu_ms\":"<<server_ms<<",\"client_cpu_ms\":"<<client_ms<<",\"upload_bytes\":"<<req_bytes.size()<<",\"download_bytes\":"<<rsp_bytes.size()<<",\"correct\":true}\n";
  }
}
}}}
