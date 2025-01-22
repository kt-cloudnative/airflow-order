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
                , WORKFLOW_NAME='PP_CSND169B',WORKFLOW_ID='80a9e533148b44d89a8823e2de2d98ee', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSND169B-0.0.prd-tz.2.3'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 21, 21, 21, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('80a9e533148b44d89a8823e2de2d98ee')

    csnd169bJob_vol = []
    csnd169bJob_volMnt = []
    csnd169bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csnd169bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csnd169bJob_env = [getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISConfigMap('icis-oder-ppon-batch-configmap2'), getICISSecret('icis-oder-ppon-batch-secret')]
    csnd169bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    csnd169bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csnd169bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '82848d5ebf9b4f1fbfd680c8488d2823',
        'volumes': csnd169bJob_vol,
        'volume_mounts': csnd169bJob_volMnt,
        'env_from':csnd169bJob_env,
        'task_id':'csnd169bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.3',
        'arguments':["--job.name=csnd169bJob", "endTranDate=20240529", "pgmNm=csnd169b", "requestDate=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('80a9e533148b44d89a8823e2de2d98ee')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csnd169bJob,
        Complete
    ]) 

    # authCheck >> csnd169bJob >> Complete
    workflow








