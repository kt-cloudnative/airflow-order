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
                , WORKFLOW_NAME='IN_CBON335B',WORKFLOW_ID='dee9e127e5cf4bc8b1972608aba1232f', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-IN_CBON335B-0.4.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 1, 11, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('dee9e127e5cf4bc8b1972608aba1232f')

    cbon335bJob_vol = []
    cbon335bJob_volMnt = []
    cbon335bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cbon335bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbon335bJob_env = [getICISConfigMap('icis-oder-intelnet-batch-configmap'), getICISConfigMap('icis-oder-intelnet-batch-configmap2'), getICISSecret('icis-oder-intelnet-batch-secret')]
    cbon335bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    cbon335bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbon335bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '4adfa9be2ac44433b1677fe845a57cbc',
        'volumes': cbon335bJob_vol,
        'volume_mounts': cbon335bJob_volMnt,
        'env_from':cbon335bJob_env,
        'task_id':'cbon335bJob',
        'image':'/icis/icis-oder-intelnet-batch:0.7.1.1',
        'arguments':["--job.name=cbon335bJob", "closeDate=${YYYYMMDD}", "empNo=cbon335b", "empNm=cbon335b", "ofcCd=7007667", "requestDate=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('dee9e127e5cf4bc8b1972608aba1232f')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbon335bJob,
        Complete
    ]) 

    # authCheck >> cbon335bJob >> Complete
    workflow








