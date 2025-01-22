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
                , WORKFLOW_NAME='PP_CSND114B',WORKFLOW_ID='0b97b7af9c91456d8d50e19a2ce0bf29', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSND114B-0.0.prd-tz.3.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 21, 20, 38, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('0b97b7af9c91456d8d50e19a2ce0bf29')

    csnd114bJob_vol = []
    csnd114bJob_volMnt = []
    csnd114bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csnd114bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csnd114bJob_env = [getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISConfigMap('icis-oder-ppon-batch-configmap2'), getICISSecret('icis-oder-ppon-batch-secret')]
    csnd114bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    csnd114bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csnd114bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '416b2e8c6f604839a320f0a9bd01f19f',
        'volumes': csnd114bJob_vol,
        'volume_mounts': csnd114bJob_volMnt,
        'env_from':csnd114bJob_env,
        'task_id':'csnd114bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.3',
        'arguments':["--job.name=csnd114bJob", "workDate=20240529", "workEmpNo=91348440", "testRequestDate=${YYYYMMDDHHMISS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('0b97b7af9c91456d8d50e19a2ce0bf29')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csnd114bJob,
        Complete
    ]) 

    # authCheck >> csnd114bJob >> Complete
    workflow








