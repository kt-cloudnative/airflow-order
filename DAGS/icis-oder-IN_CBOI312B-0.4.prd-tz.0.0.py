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
                , WORKFLOW_NAME='IN_CBOI312B',WORKFLOW_ID='5613092743ae4189839bea43ac139b2a', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-IN_CBOI312B-0.4.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 1, 11, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('5613092743ae4189839bea43ac139b2a')

    cboi312bJob_vol = []
    cboi312bJob_volMnt = []
    cboi312bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cboi312bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cboi312bJob_env = [getICISConfigMap('icis-oder-intelnet-batch-configmap'), getICISConfigMap('icis-oder-intelnet-batch-configmap2'), getICISSecret('icis-oder-intelnet-batch-secret')]
    cboi312bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    cboi312bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cboi312bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '81b2014918ab4498a3158b32c7aa68cd',
        'volumes': cboi312bJob_vol,
        'volume_mounts': cboi312bJob_volMnt,
        'env_from':cboi312bJob_env,
        'task_id':'cboi312bJob',
        'image':'/icis/icis-oder-intelnet-batch:0.7.1.1',
        'arguments':["--job.name=cboi312bJob", "endTranDate=${YYYYMMDD}", "ofcCd=CBOI312B", "requestDate=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('5613092743ae4189839bea43ac139b2a')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cboi312bJob,
        Complete
    ]) 

    # authCheck >> cboi312bJob >> Complete
    workflow








