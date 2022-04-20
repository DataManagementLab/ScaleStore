#pragma once
// -------------------------------------------------------------------------------------
#include "Defs.hpp"

// -------------------------------------------------------------------------------------

namespace scalestore
{
namespace rdma
{
// -------------------------------------------------------------------------------------
enum class MESSAGE_TYPE : uint8_t {
   Empty = 0,  // 0 initialized
   Finish = 1,
   // -------------------------------------------------------------------------------------
   // possession requests
   PRX = 2,
   PRS = 3,
   // -------------------------------------------------------------------------------------
   // possesion request responses
   PRRX = 4,
   PRRS = 5,
   // move requests
   PMR = 6,
   PMRR = 7,
   // -------------------------------------------------------------------------------------
   // copy request
   PCR = 8,
   PCRR = 9,
   // -------------------------------------------------------------------------------------
   // update request
   PUR = 10,
   PURR = 11,
   // -------------------------------------------------------------------------------------
   // remote allocation requests
   RAR = 12,
   RARR  = 13,
   // -------------------------------------------------------------------------------------
   PRFR = 14,
   PRFRR =15,
   
   // -------------------------------------------------------------------------------------
   DPMR = 96, // delegate possession request
   // Remote information for delegation
   DR = 97, 
   DRR = 98,
   // -------------------------------------------------------------------------------------   
   Init = 99,
   // -------------------------------------------------------------------------------------
   Test = 100,  
   
};
// -------------------------------------------------------------------------------------
enum class RESULT : uint8_t {
   WithPage = 1,
   NoPage = 2,
   NoPageSharedConflict = 3,     // other nodes posses the page in S mode
   NoPageExclusiveConflict = 4,  // other nodes posses the page in X mode
   WithPageSharedConflict = 5,    // other nodes posses the page but page is on owner in S mode
   NoPageEvicted = 6, // page is outdated at owner need to get from other shared
   UpdateFailed = 7,
   UpdateSucceed = 8,
   UpdateSucceedWithSharedConflict =9,
   CopyFailedWithRestart =10,
   CopyFailedWithInvalidation =11,
};
// -------------------------------------------------------------------------------------
// INIT Message is exchanged via RDMA S/R hence not in inheritance hierarchy
// -------------------------------------------------------------------------------------
struct InitMessage {
   uintptr_t mbOffset;  // rdma offsets on remote
   uintptr_t plOffset;
   uintptr_t mbResponseOffset; // for page provider only 
   uintptr_t plResponseOffset;
   NodeID bmId;  // node id of buffermanager the initiator belongs to
   MESSAGE_TYPE type = MESSAGE_TYPE::Init;
};
// -------------------------------------------------------------------------------------
// Protocol Messages
// -------------------------------------------------------------------------------------
struct Message {
   MESSAGE_TYPE type;
   Message() : type(MESSAGE_TYPE::Empty) {}
   Message(MESSAGE_TYPE type) : type(type) {}
};
// -------------------------------------------------------------------------------------
struct FinishRequest : public Message {
   FinishRequest() : Message(MESSAGE_TYPE::Finish){}
};
// -------------------------------------------------------------------------------------
struct PossessionRequest : public Message {
   PID pid;
   uintptr_t pageOffset; 
   PossessionRequest(MESSAGE_TYPE type, PID pid, uintptr_t pageOffset) : Message(type), pid(pid), pageOffset(pageOffset){}
};
struct __attribute__((packed)) PossessionResponse : public Message {
   RESULT resultType;
   uint64_t conflictingNodeId;
   uint64_t pVersion;
   bool delegated = false;
   uint8_t receiveFlag = 1; 
   PossessionResponse(RESULT result):Message(MESSAGE_TYPE::PRRS), resultType(result){};
   PossessionResponse(RESULT result, NodeID conflictingNodeId):Message(MESSAGE_TYPE::PRRS), resultType(result), conflictingNodeId(conflictingNodeId){};
};
// -------------------------------------------------------------------------------------
// moves possession to other party
struct __attribute__((packed)) PossessionMoveRequest : public Message {
   bool needPage;
   uint64_t pid;
   uintptr_t pageOffset;
   uint64_t pVersion; // own version
   PossessionMoveRequest(uint64_t pid, bool needPage, uintptr_t pageOffset, uint64_t pVersion)
       : Message(MESSAGE_TYPE::PMR), needPage(needPage), pid(pid), pageOffset(pageOffset), pVersion(pVersion) {}
};

struct PossessionMoveResponse : public Message {
   RESULT resultType;
   uint8_t receiveFlag = 1;
   PossessionMoveResponse(RESULT result) : Message(MESSAGE_TYPE::PMRR), resultType(result){}
};  

// -------------------------------------------------------------------------------------
// copys possession to other party
struct __attribute__((packed)) PossessionCopyRequest : public Message {
   uint64_t pid;
   uintptr_t pageOffset;
   uint64_t pVersion; // own version 
   PossessionCopyRequest(uint64_t pid, uintptr_t pageOffset, uint64_t pVersion) : Message(MESSAGE_TYPE::PCR), pid(pid), pageOffset(pageOffset), pVersion(pVersion){}
};

// maybe not needed
struct PossessionCopyResponse : public Message {
   RESULT resultType;
   uint8_t receiveFlag = 1;
   PossessionCopyResponse(RESULT result) : Message(MESSAGE_TYPE::PCRR), resultType(result){}
};  
// -------------------------------------------------------------------------------------

// -------------------------------------------------------------------------------------
// copys possession to other party
struct PossessionUpdateRequest : public Message {
   PID pid;
   uint64_t pVersion; // own version 
   PossessionUpdateRequest(PID pid, uint64_t pVersion ) : Message(MESSAGE_TYPE::PUR), pid(pid), pVersion(pVersion){}
};

// maybe not needed
struct __attribute__((packed)) PossessionUpdateResponse : public Message  {
   RESULT resultType;
   uint64_t conflictingNodeId;
   uint64_t pVersion; // own version 
   uint8_t receiveFlag = 1;
   PossessionUpdateResponse(RESULT result) : Message(MESSAGE_TYPE::PURR), resultType(result){}
};

// -------------------------------------------------------------------------------------
// remote allocation request
struct RemoteAllocationRequest : public Message {
   RemoteAllocationRequest() : Message(MESSAGE_TYPE::RAR){}
};

struct RemoteAllocationResponse : public Message  {
   PID pid;
   uint8_t receiveFlag = 1;
   RemoteAllocationResponse(PID pid) : Message(MESSAGE_TYPE::RARR), pid(pid){}
};


// -------------------------------------------------------------------------------------
// Deledation information only send once for setup
// transfer other MH information to MH in order to allow MH to delegate 
struct DelegationRequest : public Message {
   uintptr_t mbOffset;
   uintptr_t mbPayload;
   uint64_t bmId;
   DelegationRequest(uintptr_t mbOffset, uintptr_t mbPayload, uint64_t bmId) : Message(MESSAGE_TYPE::DR), mbOffset(mbOffset), mbPayload(mbPayload), bmId(bmId){}
};

struct DelegationResponse : public Message  {
   uint8_t receiveFlag = 1;
   DelegationResponse() : Message(MESSAGE_TYPE::DRR){}
};

// -------------------------------------------------------------------------------------
// Get size of Largest Message
union ALLDERIVED{
   FinishRequest fm;
   PossessionResponse pr;
   PossessionRequest preq;
   PossessionMoveResponse pmr;
   PossessionMoveRequest pmrr;
   PossessionCopyResponse pcr;
   PossessionCopyRequest pcrr;
   RemoteAllocationRequest rar;
   RemoteAllocationResponse rarr;
   DelegationRequest dr;
   DelegationResponse drr;
};

static constexpr uint64_t LARGEST_MESSAGE = sizeof(ALLDERIVED);
static_assert(LARGEST_MESSAGE <= 32, "Messags span more than one CL");


struct MessageFabric{
   template<typename T, class... Args>
   static T* createMessage(void* buffer, Args&&... args){
      return new (buffer) T (args...);
   }   
};

}  // namespace rdma
}  // namespace scalestore
