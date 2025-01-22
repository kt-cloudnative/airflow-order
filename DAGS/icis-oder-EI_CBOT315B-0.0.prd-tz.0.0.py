from datetime import datetime, timedelta
from kubernetes.client import models as k8s
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.subdag import SubDagOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.helpers import chain, cross_downstream
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
import pendulum
local_tz = pendulum.timezone("Asia/Seoul")
import sys
sys.path.append('/opt/bitnami/airflow/dags/git_sa-common')

from icis_common import *
COMMON = ICISCmmn(DOMAIN='oder',ENV='prd-tz', NAMESPACE='t-order'
                , WORKFLOW_NAME='EI_CBOT315B',WORKFLOW_ID='ab9121be350342518969358267b18652', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-EI_CBOT315B-0.0.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 7, 16, 11, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('ab9121be350342518969358267b18652')

    cbot315bJob_vol = []
    cbot315bJob_volMnt = []
    cbot315bJob_env = [getICISConfigMap('icis-oder-entprinet-batch-configmap'), getICISConfigMap('icis-oder-entprinet-batch-configmap2'), getICISSecret('icis-oder-entprinet-batch-secret')]
    cbot315bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    cbot315bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbot315bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'd023a1e842424f02bc25a646c4895f46',
        'volumes': cbot315bJob_vol,
        'volume_mounts': cbot315bJob_volMnt,
        'env_from':cbot315bJob_env,
        'task_id':'cbot315bJob',
        'image':'/icis/icis-oder-entprinet-batch:0.4.1.14',
        'arguments':["--job.name=cbot315bJob", "requestDate="+str(datetime.now()), "endTranDate="+str(datetime.now().strftime("%Y%m%d")), "empNo=cbot315b", "empName=cbot315b", "ofcCd=481967", "progName=cbot315b"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    cbod445bJob_vol = []
    cbod445bJob_volMnt = []
    cbod445bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cbod445bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbod445bJob_env = [getICISConfigMap('icis-oder-entprinet-batch-configmap'), getICISConfigMap('icis-oder-entprinet-batch-configmap2'), getICISSecret('icis-oder-entprinet-batch-secret')]
    cbod445bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    cbod445bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbod445bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '4f7b4221dc1d446db535ad16ceecf925',
        'volumes': cbod445bJob_vol,
        'volume_mounts': cbod445bJob_volMnt,
        'env_from':cbod445bJob_env,
        'task_id':'cbod445bJob',
        'image':'/icis/icis-oder-entprinet-batch:0.4.1.14',
        'arguments':["--job.name=cbod445bJob", "requestDate="+str(datetime.now()), "procDate="+str(datetime.now().strftime("%Y%m%d")) ,"progName=cbod445b"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('ab9121be350342518969358267b18652')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbot315bJob,
        cbod445bJob,
        Complete
    ]) 

    # authCheck >> cbot315bJob >> cbod445bJob >> Complete
    workflow








